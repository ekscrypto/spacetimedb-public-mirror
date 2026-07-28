use netstat2::{get_sockets_info, AddressFamilyFlags, ProtocolFlags, ProtocolSocketInfo, TcpState};
use spacetimedb_client_api::routes::identity::IdentityRoutes;
use spacetimedb_pg::pg_server;
use std::io::{self, Write};
use std::net::IpAddr;
use std::sync::Arc;

use crate::{StandaloneEnv, StandaloneOptions};
use anyhow::Context;
use axum::extract::DefaultBodyLimit;
use clap::ArgAction::SetTrue;
use clap::{Arg, ArgMatches};
use spacetimedb::config::{parse_config, CertificateAuthority};
use spacetimedb::db::persistence::{CommitlogConfig, DurabilityConfig};
use spacetimedb::db::{self, Storage};
use spacetimedb::host::MirrorPolicy;
use spacetimedb::messages::control_db::{Database, HostType, Replica};
use spacetimedb::startup::{self, TracingOptions};
use spacetimedb::util::jobs::JobCores;
use spacetimedb::worker_metrics;
use spacetimedb::Identity;
use spacetimedb_client_api::routes::database::DatabaseRoutes;
use spacetimedb_client_api::routes::router;
use spacetimedb_client_api_messages::name::DatabaseName;
use spacetimedb_public_mirror_client::runtime::{run_public_mirror_loop, schema_program_hash, PublicMirrorConfig};
use spacetimedb_public_mirror_client::schema::fetch_and_parse_schema;
use spacetimedb_public_mirror_client::schema::public_user_table_names;
use std::str::FromStr;
use std::time::Duration;
use url::Url;
use spacetimedb_client_api::routes::subscribe::WebSocketOptions;
use spacetimedb_paths::cli::{PrivKeyPath, PubKeyPath};
use spacetimedb_paths::server::{ConfigToml, ServerDataDir};
use tokio::net::TcpListener;

pub fn cli() -> clap::Command {
    clap::Command::new("start")
        .about("Starts a standalone SpacetimeDB instance")
        .args_override_self(true)
        .override_usage("spacetime start [OPTIONS]")
        .arg(
            Arg::new("listen_addr")
                .long("listen-addr")
                .short('l')
                .default_value("0.0.0.0:3000")
                .help(
                    "The address and port where SpacetimeDB should listen for connections. \
                     This defaults to listening on all IP addresses on port 3000.",
                ),
        )
        .arg(
            Arg::new("data_dir")
                .long("data-dir")
                .help("The path to the data directory for the database")
                .required(true)
                .value_parser(clap::value_parser!(ServerDataDir)),
        )
        .arg(
            Arg::new("enable_tracy")
                .long("enable-tracy")
                .action(SetTrue)
                .help("Enable Tracy profiling"),
        )
        .arg(
            Arg::new("jwt_key_dir")
                .hide(true)
                .long("jwt-key-dir")
                .help("The directory with id_ecdsa and id_ecdsa.pub")
                .value_parser(clap::value_parser!(spacetimedb_paths::cli::ConfigDir)),
        )
        .arg(
            Arg::new("jwt_pub_key_path")
                .long("jwt-pub-key-path")
                .requires("jwt_priv_key_path")
                .help("The path to the public jwt key for verifying identities")
                .value_parser(clap::value_parser!(PubKeyPath)),
        )
        .arg(
            Arg::new("jwt_priv_key_path")
                .long("jwt-priv-key-path")
                .requires("jwt_pub_key_path")
                .help("The path to the private jwt key for issuing identities")
                .value_parser(clap::value_parser!(PrivKeyPath)),
        )
        .arg(Arg::new("in_memory").long("in-memory").action(SetTrue).help(
            "If specified the database will run entirely in memory. After the process exits all data will be lost.",
        ))
        .arg(
            Arg::new("page_pool_max_size").long("page_pool_max_size").help(
                "The maximum size of the page pool in bytes. Should be a multiple of 64KiB. The default is 8GiB.",
            ),
        )
        .arg(
            Arg::new("pg_port")
                .long("pg-port")
                .help("If specified, enables the built-in PostgreSQL wire protocol server on the given port.")
                .value_parser(clap::value_parser!(u16).range(1024..65535)),
        )
        .arg(
            Arg::new("non_interactive")
                .long("non-interactive")
                .action(SetTrue)
                .help("Run in non-interactive mode (fail immediately if port is in use)"),
        )
        .arg(
            Arg::new("public_mirror_v1")
                .long("public-mirror-v1")
                .action(SetTrue)
                .help(
                    "Run as an in-memory public-mirror-v1 of a remote v1 BSATN database \
                     (forces in-memory storage; rejects CallReducer)",
                ),
        )
        .arg(
            Arg::new("mirror")
                .long("mirror")
                .help(
                    "Upstream to mirror as <upstream-url>/<database-name> (repeatable). \
                     Example: wss://host.example/bitcraft-live-1. Requires --public-mirror-v1.",
                )
                .requires("public_mirror_v1")
                .action(clap::ArgAction::Append),
        )
        .arg(
            Arg::new("mirror_token")
                .long("mirror-token")
                .help(
                    "Bearer token for upstream auth (also BITCRAFT_TOKEN or MIRROR_TOKEN env). \
                     Optional for --public-mirror-v1; shared by all --mirror entries",
                )
                .requires("public_mirror_v1"),
        )
        .arg(
            Arg::new("mirror_token_file")
                .long("mirror-token-file")
                .help(
                    "Path to a file containing the upstream bearer token (also MIRROR_TOKEN_FILE env). \
                     Multi-line files are supported: the first eyJ… JWT line is used.",
                )
                .requires("public_mirror_v1")
                .value_parser(clap::value_parser!(std::path::PathBuf)),
        )
        .arg(
            Arg::new("mirror_table")
                .long("mirror-table")
                .help(
                    "Restrict upstream subscribe to these public tables (repeatable; applies to every --mirror). \
                     Default: all public user tables.",
                )
                .requires("public_mirror_v1")
                .action(clap::ArgAction::Append),
        )
        .arg(
            Arg::new("reject_one_off_query")
                .long("reject-one-off-query")
                .action(SetTrue)
                .help("In public-mirror-v1 mode, also reject OneOffQuery (allowed by default)"),
        )
    // .after_help("Run `spacetime help start` for more detailed information.")
}

#[derive(Default, serde::Deserialize)]
struct ConfigFile {
    #[serde(flatten)]
    common: spacetimedb::config::ConfigFile,
    #[serde(default)]
    commitlog: CommitlogConfig,
    #[serde(default)]
    websocket: WebSocketOptions,
}

impl ConfigFile {
    fn read(path: &ConfigToml) -> anyhow::Result<Option<Self>> {
        parse_config(path.as_ref())
    }
}

pub async fn exec(args: &ArgMatches, db_cores: JobCores) -> anyhow::Result<()> {
    let listen_addr = args.get_one::<String>("listen_addr").unwrap();
    let pg_port = args.get_one::<u16>("pg_port");
    let non_interactive = args.get_flag("non_interactive");
    let public_mirror_v1 = args.get_flag("public_mirror_v1");
    let reject_one_off_query = args.get_flag("reject_one_off_query");
    let cert_dir = args.get_one::<spacetimedb_paths::cli::ConfigDir>("jwt_key_dir");
    let certs = Option::zip(
        args.get_one::<PubKeyPath>("jwt_pub_key_path").cloned(),
        args.get_one::<PrivKeyPath>("jwt_priv_key_path").cloned(),
    )
    .map(|(jwt_pub_key_path, jwt_priv_key_path)| CertificateAuthority {
        jwt_pub_key_path,
        jwt_priv_key_path,
    });
    let data_dir = args.get_one::<ServerDataDir>("data_dir").unwrap();
    let enable_tracy = args.get_flag("enable_tracy") || std::env::var_os("SPACETIMEDB_TRACY").is_some();

    let storage = if public_mirror_v1 {
        if !args.get_flag("in_memory") {
            log::info!("--public-mirror-v1 forces in-memory storage");
        }
        Storage::Memory
    } else if args.get_flag("in_memory") {
        Storage::Memory
    } else {
        Storage::Disk
    };

    let mirror_tables: Option<Vec<String>> = args
        .get_many::<String>("mirror_table")
        .map(|vals| vals.cloned().collect());
    let mirror_token = resolve_mirror_token(args)?;
    let mirrors = if public_mirror_v1 {
        let raw: Vec<String> = args
            .get_many::<String>("mirror")
            .map(|vals| vals.cloned().collect())
            .unwrap_or_default();
        anyhow::ensure!(
            !raw.is_empty(),
            "--public-mirror-v1 requires at least one --mirror <upstream-url>/<database>"
        );
        Some(parse_mirror_specs(&raw)?)
    } else {
        None
    };

    let page_pool_max_size = args
        .get_one::<String>("page_pool_max_size")
        .map(|size| parse_size::Config::new().with_binary().parse_size(size))
        .transpose()
        .context("unrecognized format in `page_pool_max_size`")?
        .map(|size| size as usize);
    let db_config = db::Config {
        storage,
        page_pool_max_size,
    };

    banner();
    let exe_name = std::env::current_exe()?;
    let exe_name = exe_name.file_name().unwrap().to_str().unwrap();
    println!("{} version: {}", exe_name, env!("CARGO_PKG_VERSION"));
    println!("{} path: {}", exe_name, std::env::current_exe()?.display());
    println!("database running in data directory {}", data_dir.display());
    if let Some(ref mirrors) = mirrors {
        println!("public-mirror-v1 mode: mirroring {} database(s):", mirrors.len());
        for m in mirrors {
            println!("  {} from {}", m.database, m.upstream);
        }
    }

    let config_path = data_dir.config_toml();
    let config = match ConfigFile::read(&data_dir.config_toml())? {
        Some(config) => config,
        None => {
            let default_config = include_str!("../../config.toml");
            data_dir.create()?;
            config_path.write(default_config)?;
            toml::from_str(default_config).unwrap()
        }
    };

    startup::configure_tracing(TracingOptions {
        config: config.common.logs,
        reload_config: cfg!(debug_assertions).then_some(config_path),
        disk_logging: std::env::var_os("SPACETIMEDB_DISABLE_DISK_LOGGING")
            .is_none()
            .then(|| data_dir.logs()),
        edition: "standalone".to_owned(),
        tracy: enable_tracy || std::env::var_os("SPACETIMEDB_TRACY").is_some(),
        flamegraph: std::env::var_os("SPACETIMEDB_FLAMEGRAPH").map(|_| {
            std::env::var_os("SPACETIMEDB_FLAMEGRAPH_PATH")
                .unwrap_or("/var/log/flamegraph.folded".into())
                .into()
        }),
    });

    let certs = certs
        .or(config.common.certificate_authority)
        .or_else(|| cert_dir.map(CertificateAuthority::in_cli_config_dir))
        .context("cannot omit --jwt-{pub,priv}-key-path when those options are not specified in config.toml")?;

    let data_dir = Arc::new(data_dir.clone());
    let ctx = StandaloneEnv::init(
        StandaloneOptions {
            db_config,
            durability: DurabilityConfig {
                commitlog: config.commitlog,
            },
            websocket: config.websocket,
            wasm: config.common.wasm,
            v8: config.common.v8,
        },
        &certs,
        data_dir,
        db_cores,
    )
    .await?;
    worker_metrics::spawn_jemalloc_stats(listen_addr.clone());
    worker_metrics::spawn_tokio_stats(
        listen_addr.clone(),
        "main".to_string(),
        tokio::runtime::Handle::current(),
    );
    worker_metrics::spawn_page_pool_stats(listen_addr.clone(), ctx.page_pool().clone());
    worker_metrics::spawn_bsatn_rlb_pool_stats(listen_addr.clone(), ctx.bsatn_rlb_pool().clone());

    if let Some(mirrors) = mirrors {
        for m in &mirrors {
            bootstrap_public_mirror(
                &ctx,
                &m.upstream,
                &m.database,
                mirror_token.as_deref(),
                mirror_tables.clone(),
                reject_one_off_query,
            )
            .await
            .with_context(|| format!("failed to bootstrap public-mirror for `{}`", m.database))?;
        }
    }

    let mut db_routes = DatabaseRoutes::default();
    db_routes.root_post = db_routes.root_post.layer(DefaultBodyLimit::disable());
    db_routes.db_put = db_routes.db_put.layer(DefaultBodyLimit::disable());
    db_routes.pre_publish = db_routes.pre_publish.layer(DefaultBodyLimit::disable());
    let extra = axum::Router::new()
        .nest("/health", spacetimedb_client_api::routes::health::router())
        .nest("/mirrors", spacetimedb_client_api::routes::mirrors::router());
    let service = router(&ctx, db_routes, IdentityRoutes::default(), extra).with_state(ctx.clone());

    // Check if the requested port is available on both IPv4 and IPv6.
    // If not, offer to find an available port by incrementing (unless non-interactive).
    let listen_addr = if let Some((host, port_str)) = listen_addr.rsplit_once(':') {
        if let Ok(requested_port) = port_str.parse::<u16>() {
            if !is_port_available(host, requested_port) {
                if non_interactive {
                    anyhow::bail!(
                        "Port {} is already in use. Please free up the port or specify a different port with --listen-addr.",
                        requested_port
                    );
                }
                // Port is in use, try to find an alternative
                match find_available_port(host, requested_port.saturating_add(1), 100) {
                    Some(available_port) => {
                        let question = format!(
                            "Port {} is already in use. Would you like to use port {} instead?",
                            requested_port, available_port
                        );
                        if prompt_yes_no(&question) {
                            format!("{}:{}", host, available_port)
                        } else {
                            anyhow::bail!(
                                "Port {} is already in use. Please free up the port or specify a different port with --listen-addr.",
                                requested_port
                            );
                        }
                    }
                    None => {
                        anyhow::bail!(
                            "Port {} is already in use and could not find an available port nearby. \
                             Please free up the port or specify a different port with --listen-addr.",
                            requested_port
                        );
                    }
                }
            } else {
                listen_addr.to_string()
            }
        } else {
            listen_addr.to_string()
        }
    } else {
        listen_addr.to_string()
    };

    let tcp = TcpListener::bind(&listen_addr).await.context(format!(
        "failed to bind the SpacetimeDB server to '{listen_addr}', please check that the address is valid and not already in use"
    ))?;
    socket2::SockRef::from(&tcp).set_nodelay(true)?;
    log::info!("Starting SpacetimeDB listening on {}", tcp.local_addr()?);

    if let Some(pg_port) = pg_port {
        let server_addr = listen_addr.split(':').next().unwrap();
        let tcp_pg = TcpListener::bind(format!("{server_addr}:{pg_port}")).await.context(format!(
            "failed to bind the SpacetimeDB PostgreSQL wire protocol server to {server_addr}:{pg_port}, please check that the port is valid and not already in use"
        ))?;

        let notify = Arc::new(tokio::sync::Notify::new());
        let shutdown_notify = notify.clone();
        tokio::select! {
            _ = pg_server::start_pg(notify.clone(), ctx, tcp_pg) => {},
            _ = axum::serve(tcp, service).with_graceful_shutdown(async move  {
                shutdown_notify.notified().await;
            }) => {},
            _ = tokio::signal::ctrl_c() => {
                println!("Shutting down servers...");
                notify.notify_waiters(); // Notify all tasks
            }
        }
    } else {
        log::warn!("PostgreSQL wire protocol server disabled");
        axum::serve(tcp, service)
            .with_graceful_shutdown(async {
                tokio::signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
                log::info!("Shutting down server...");
            })
            .await?;
    }

    Ok(())
}

/// Check if a port is available on the requested host for both IPv4 and IPv6.
///
/// On macOS (and some other systems), `localhost` can resolve to both IPv4 (127.0.0.1)
/// and IPv6 (::1). If SpacetimeDB binds only to IPv4 but another service is using the
/// same port on IPv6, browsers may connect to the wrong service depending on which
/// address they try first.
///
/// This function checks both the requested IPv4 address and its IPv6 equivalent:
/// - 127.0.0.1 -> also checks ::1
/// - 0.0.0.0 -> also checks ::
/// - 10.1.1.1 -> also checks ::ffff:10.1.1.1 (IPv4-mapped IPv6)
///
/// Note: There is a small race condition between this check and the actual bind -
/// another process could grab the port in between. This is unlikely in practice
/// and the actual bind will fail with a clear error if it happens.
pub fn is_port_available(host: &str, port: u16) -> bool {
    let requested = match parse_host(host) {
        Some(r) => r,
        None => return false, // invalid host string => treat as not available
    };

    let sockets = match get_sockets_info(AddressFamilyFlags::IPV4 | AddressFamilyFlags::IPV6, ProtocolFlags::TCP) {
        Ok(s) => s,
        Err(_) => {
            log::warn!("Unable to check whether port {port} is available. Proceeding as though it is.");
            // Default to allowing, because otherwise we can have cases where users are entirely unable to start servers.
            // See https://github.com/clockworklabs/SpacetimeDB/issues/5556.
            return true;
        }
    };

    for si in sockets {
        let tcp = match si.protocol_socket_info {
            ProtocolSocketInfo::Tcp(tcp_si) => tcp_si,
            _ => continue,
        };

        if tcp.state != TcpState::Listen {
            continue;
        }

        if tcp.local_port != port {
            continue;
        }

        if conflicts(requested, tcp.local_addr) {
            return false;
        }
    }

    true
}

#[derive(Debug, Clone, Copy)]
enum RequestedHost {
    Localhost,
    Ip(IpAddr),
}

fn parse_host(host: &str) -> Option<RequestedHost> {
    let host = host.trim();

    // Allow common bracketed IPv6 formats like "[::1]"
    let host = host.strip_prefix('[').and_then(|s| s.strip_suffix(']')).unwrap_or(host);

    if host.eq_ignore_ascii_case("localhost") {
        return Some(RequestedHost::Localhost);
    }

    host.parse::<IpAddr>().ok().map(RequestedHost::Ip)
}

fn conflicts(requested: RequestedHost, listener_addr: IpAddr) -> bool {
    match requested {
        RequestedHost::Localhost => match listener_addr {
            // localhost should conflict with loopback and wildcards in each family
            IpAddr::V4(v4) => v4.is_loopback() || v4.is_unspecified(),
            IpAddr::V6(v6) => v6.is_loopback() || v6.is_unspecified(),
        },

        RequestedHost::Ip(IpAddr::V4(req_v4)) => match listener_addr {
            IpAddr::V4(l_v4) => {
                if req_v4.is_unspecified() {
                    // 0.0.0.0 conflicts with any IPv4 listener
                    true
                } else if req_v4.is_loopback() {
                    // 127.0.0.1 conflicts with 127.0.0.1 and 0.0.0.0
                    l_v4 == req_v4 || l_v4.is_unspecified()
                } else {
                    // specific IPv4 conflicts with that IPv4 and 0.0.0.0
                    l_v4 == req_v4 || l_v4.is_unspecified()
                }
            }
            IpAddr::V6(l_v6) => {
                if req_v4.is_unspecified() {
                    // special case: 0.0.0.0 conflicts with :: (and vice versa)
                    l_v6.is_unspecified()
                } else if req_v4.is_loopback() {
                    // special case: 127.0.0.1 conflicts with ::1 (and vice versa)
                    l_v6.is_loopback()
                        // and treat IPv6 wildcard as conflicting with IPv4 loopback per your table
                        || l_v6.is_unspecified()
                        // also consider rare IPv4-mapped IPv6 listeners
                        || l_v6.to_ipv4_mapped() == Some(req_v4)
                } else {
                    // specific IPv4 should conflict with IPv6 wildcard (::) per your table
                    l_v6.is_unspecified() || l_v6.to_ipv4_mapped() == Some(req_v4)
                }
            }
        },

        RequestedHost::Ip(IpAddr::V6(req_v6)) => match listener_addr {
            IpAddr::V6(l_v6) => {
                if req_v6.is_unspecified() {
                    // :: conflicts with any IPv6 listener
                    true
                } else if req_v6.is_loopback() {
                    // ::1 conflicts with ::1 and :: (and also with 127.0.0.1 via IPv4 branch below)
                    l_v6 == req_v6 || l_v6.is_unspecified()
                } else {
                    // specific IPv6 conflicts with itself and ::
                    l_v6 == req_v6 || l_v6.is_unspecified()
                }
            }
            IpAddr::V4(l_v4) => {
                if req_v6.is_unspecified() {
                    // :: conflicts with any IPv4 listener (matches your table)
                    true
                } else if req_v6.is_loopback() {
                    // special case: ::1 conflicts with 127.0.0.1 (and vice versa)
                    l_v4.is_loopback()
                } else {
                    // Not required by your rules: specific IPv6 does NOT conflict with IPv4 listeners.
                    false
                }
            }
        },
    }
}

/// Find an available port starting from the requested port.
/// Returns the first port that is available on both IPv4 and IPv6.
fn find_available_port(host: &str, requested_port: u16, max_attempts: u16) -> Option<u16> {
    for offset in 0..max_attempts {
        let port = requested_port.saturating_add(offset);
        if port == 0 || port == u16::MAX {
            break;
        }
        if is_port_available(host, port) {
            return Some(port);
        }
    }
    None
}

/// Prompt the user with a yes/no question. Returns true if they answer yes.
fn prompt_yes_no(question: &str) -> bool {
    print!("{} [y/N] ", question);
    io::stdout().flush().ok();

    let mut input = String::new();
    if io::stdin().read_line(&mut input).is_err() {
        return false;
    }

    matches!(input.trim().to_lowercase().as_str(), "y" | "yes")
}

/// One `--mirror <upstream-url>/<database-name>` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MirrorSpec {
    upstream: Url,
    database: String,
}

/// Parse `--mirror` values of the form `<upstream-url>/<database-name>`.
///
/// The last non-empty path segment is the database name; the URL with that
/// segment stripped is the upstream host (scheme + authority + any path prefix).
fn parse_mirror_spec(raw: &str) -> anyhow::Result<MirrorSpec> {
    let url = Url::parse(raw).with_context(|| format!("invalid --mirror URL `{raw}`"))?;
    let mut segments: Vec<String> = url
        .path_segments()
        .ok_or_else(|| anyhow::anyhow!("--mirror `{raw}` must be an absolute URL with a path"))?
        .filter(|p| !p.is_empty())
        .map(str::to_owned)
        .collect();
    let database = segments
        .pop()
        .ok_or_else(|| anyhow::anyhow!("--mirror `{raw}` must end with /<database-name>"))?;

    let mut upstream = url.clone();
    {
        let mut path = upstream.path_segments_mut().map_err(|_| {
            anyhow::anyhow!("--mirror `{raw}`: cannot modify path (is the URL a base URL?)")
        })?;
        path.clear();
        for seg in &segments {
            path.push(seg);
        }
        // Keep a trailing slash so `wss://host/` stays a valid join base.
        path.push("");
    }

    Ok(MirrorSpec { upstream, database })
}

fn parse_mirror_specs(raw: &[String]) -> anyhow::Result<Vec<MirrorSpec>> {
    let mut out = Vec::with_capacity(raw.len());
    let mut seen = std::collections::HashSet::new();
    for entry in raw {
        let spec = parse_mirror_spec(entry)?;
        if !seen.insert(spec.database.clone()) {
            anyhow::bail!(
                "duplicate --mirror database name `{}` (each local mirror must have a unique name)",
                spec.database
            );
        }
        out.push(spec);
    }
    Ok(out)
}

async fn bootstrap_public_mirror(
    ctx: &StandaloneEnv,
    upstream_url: &Url,
    mirror_database: &str,
    token: Option<&str>,
    tables: Option<Vec<String>>,
    reject_one_off_query: bool,
) -> anyhow::Result<()> {
    log::info!("public-mirror: fetching schema for {mirror_database} from {upstream_url}");
    let (schema_bytes, module_def) = fetch_and_parse_schema(upstream_url, mirror_database)
        .await
        .context("failed to fetch/parse upstream schema")?;
    log::info!(
        "public-mirror: schema fetched ({} bytes, {} tables)",
        schema_bytes.len(),
        module_def.tables().count()
    );

    let database_identity = Identity::from_claims("public-mirror-v1", mirror_database);
    let owner_identity = Identity::from_claims("public-mirror-v1", "owner");
    let initial_program = schema_program_hash(&schema_bytes);

    let database = Database {
        id: 0,
        database_identity,
        owner_identity,
        host_type: HostType::Mirror,
        initial_program,
        bootstrap_generation: 0,
    };

    let control = ctx.control_db();
    let database_id = control
        .insert_database(database.clone())
        .context("failed to insert mirror database into control_db")?;
    let mut database = control
        .get_database_by_id(database_id)?
        .context("mirror database missing after insert")?;

    // Ensure clients can connect by name.
    let domain = DatabaseName::from_str(mirror_database)
        .with_context(|| format!("invalid --mirror database name `{mirror_database}`"))?
        .into();
    match control.spacetime_insert_domain(&database_identity, domain, owner_identity, true) {
        Ok(_) => log::info!("public-mirror: registered domain `{mirror_database}`"),
        Err(crate::control_db::Error::RecordAlreadyExists(_)) => {
            log::info!("public-mirror: domain `{mirror_database}` already registered");
        }
        Err(e) => return Err(e.into()),
    }

    // Insert leader replica without triggering get_or_launch (which rejects HostType::Mirror).
    let replica_id = control.insert_replica(Replica {
        id: 0,
        database_id,
        node_id: 0,
        leader: true,
    })?;
    log::info!("public-mirror: control_db database_id={database_id} replica_id={replica_id}");

    database.id = database_id;
    let module_host = ctx
        .host_controller()
        .bootstrap_mirror_database(
            database,
            replica_id,
            module_def.clone(),
            MirrorPolicy { reject_one_off_query },
        )
        .await
        .context("bootstrap_mirror_database failed")?;

    let tables_total = match &tables {
        Some(t) if !t.is_empty() => t.len() as u32,
        _ => public_user_table_names(&module_def).len() as u32,
    };
    let status = ctx
        .mirror_status_registry()
        .register(upstream_url, mirror_database, tables_total);

    let mirror_cfg = PublicMirrorConfig {
        upstream: upstream_url.clone(),
        database: mirror_database.to_string(),
        auth_token: token.map(str::to_string),
        tables,
        connect_timeout: Duration::from_secs(60),
    };
    tokio::spawn(async move {
        if let Err(e) = run_public_mirror_loop(module_host, mirror_cfg, module_def, status).await {
            log::error!("public-mirror upstream loop terminated: {e:#}");
        }
    });
    log::info!("public-mirror: upstream apply loop spawned for `{mirror_database}`");
    Ok(())
}

/// Resolve upstream bearer token from CLI / env / token file.
///
/// Multi-line files (e.g. identity hex + JWT) are supported: the first `eyJ…`
/// line is used. A leading `Bearer ` prefix is stripped.
fn resolve_mirror_token(args: &ArgMatches) -> anyhow::Result<Option<String>> {
    if let Some(tok) = args.get_one::<String>("mirror_token") {
        return Ok(Some(normalize_mirror_token(tok)));
    }
    if let Ok(tok) = std::env::var("BITCRAFT_TOKEN") {
        return Ok(Some(normalize_mirror_token(&tok)));
    }
    if let Ok(tok) = std::env::var("MIRROR_TOKEN") {
        return Ok(Some(normalize_mirror_token(&tok)));
    }
    let file = args
        .get_one::<std::path::PathBuf>("mirror_token_file")
        .cloned()
        .or_else(|| std::env::var_os("MIRROR_TOKEN_FILE").map(std::path::PathBuf::from));
    if let Some(path) = file {
        let contents = std::fs::read_to_string(&path)
            .with_context(|| format!("read mirror token file {}", path.display()))?;
        return Ok(Some(normalize_mirror_token(&contents)));
    }
    Ok(None)
}

fn normalize_mirror_token(raw: &str) -> String {
    let trimmed = raw.trim();
    // Prefer an explicit JWT line inside multi-line developer-token files.
    // Split on ASCII newlines and Unicode line/paragraph separators (U+2028/U+2029),
    // which some clipboard/export paths insert instead of `\n`.
    for line in trimmed.split(|c: char| matches!(c, '\n' | '\r' | '\u{2028}' | '\u{2029}')) {
        let line = line.trim();
        if line.starts_with("eyJ") {
            return line.to_string();
        }
    }
    let tok = trimmed
        .strip_prefix("Bearer ")
        .or_else(|| trimmed.strip_prefix("bearer "))
        .unwrap_or(trimmed)
        .trim();
    tok.to_string()
}

fn banner() {
    println!(
        r#"
┌───────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                                       │
│                                                                                                       │
│                                                                              ⢀⠔⠁                      │
│                                                                            ⣠⡞⠁                        │
│                                              ⣀⣀⣤⣤⣤⣤⣤⣤⣤⣤⣤⣤⣀⣀⣀⣀⣀⣀⣀⣤⣤⡴⠒    ⢀⣠⡾⠋                          │
│                                         ⢀⣤⣶⣾88888888888888888888⠿⠋    ⢀⣴8⡟⠁                           │
│                                      ⢀⣤⣾88888⡿⠿⠛⠛⠛⠛⠛⠛⠛⠛⠻⠿88888⠟⠁    ⣠⣾88⡟                             │
│                                    ⢀⣴88888⠟⠋⠁ ⣀⣤⠤⠶⠶⠶⠶⠶⠤⣤⣀ ⠉⠉⠉    ⢀⣴⣾888⡟                              │
│                                   ⣠88888⠋  ⣠⠶⠋⠉         ⠉⠙⠶⣄   ⢀⣴888888⠃                              │
│                                  ⣰8888⡟⠁ ⣰⠟⠁               ⠈⠻⣆ ⠈⢿888888                               │
│                                 ⢠8888⡟  ⡼⠁                   ⠈⢧ ⠈⢿8888⡿                               │
│                                 ⣼8888⠁ ⢸⠇                     ⠸⡇ ⠘8888⣷                               │
│                                 88888  8                       8  88888                               │
│                                 ⢿8888⡄ ⢸⡆                     ⢰⡇ ⢀8888⡟                               │
│                                 ⣾8888⣷⡀ ⢳⡀                   ⢀⡞  ⣼8888⠃                               │
│                                 888888⣷⡀ ⠹⣦⡀               ⢀⣴⠏ ⢀⣼8888⠏                                │
│                                ⢠888888⠟⠁   ⠙⠶⣄⣀         ⣀⣠⠶⠋  ⣠88888⠋                                 │
│                                ⣼888⡿⠟⠁    ⣀⣀⣀ ⠉⠛⠒⠶⠶⠶⠶⠶⠒⠛⠉ ⢀⣠⣴88888⠟⠁                                  │
│                               ⣼88⡿⠋    ⢀⣴88888⣶⣦⣤⣤⣤⣤⣤⣤⣤⣤⣶⣾88888⡿⠛⠁                                    │
│                             ⢀⣼8⠟⠁    ⣠⣶88888888888888888888⡿⠿⠛⠁                                       │
│                            ⣠⡾⠋⠁    ⠤⠞⠛⠛⠉⠉⠉⠉⠉⠉⠉⠛⠛⠛⠛⠛⠛⠛⠛⠛⠛⠉⠉                                            │
│                          ⢀⡼⠋                                                                          │
│                        ⢀⠔⠁                                                                            │
│                                                                                                       │
│                                                                                                       │
│  .d8888b.                                     888    d8b                        8888888b.  888888b.   │
│ d88P  Y88b                                    888    Y8P                        888  "Y88b 888  "88b  │
│ Y88b.                                         888                               888    888 888  .88P  │
│  "Y888b.   88888b.   8888b.   .d8888b .d88b.  888888 888 88888b.d88b.   .d88b.  888    888 8888888K.  │
│     "Y88b. 888 "88b     "88b d88P"   d8P  Y8b 888    888 888 "888 "88b d8P  Y8b 888    888 888  "Y88b │
│       "888 888  888 .d888888 888     88888888 888    888 888  888  888 88888888 888    888 888    888 │
│ Y88b  d88P 888 d88P 888  888 Y88b.   Y8b.     Y88b.  888 888  888  888 Y8b.     888  .d88P 888   d88P │
│  "Y8888P"  88888P"  "Y888888  "Y8888P "Y8888   "Y888 888 888  888  888  "Y8888  8888888P"  8888888P"  │
│            888                                                                                        │
│            888                                                                                        │
│            888                                                                                        │
│                                  "Development at the speed of light"                                  │
└───────────────────────────────────────────────────────────────────────────────────────────────────────┘
    "#
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn options_from_partial_toml() {
        let toml = r#"
            [logs]
            directives = [
                "banana_shake=strawberry",
            ]

            [websocket]
            idle-timeout = "1min"
            close-handshake-timeout = "500ms"

            [wasm]
            procedure-instance-pool-size = 4

            [v8]
            procedure-instance-pool-size = 3

            [v8-heap-policy]
            heap-check-request-interval = 0
            heap-check-time-interval = "45s"
            heap-gc-trigger-fraction = 0.6
            heap-retire-fraction = 0.8
            heap-limit-mb = 128

            [commitlog]
            log-format-version = 1
            max-segment-size = 1048576
            offset-index-interval-bytes = 8192
            offset-index-require-segment-fsync = false
            preallocate-segments = true
            write-buffer-size = 131072
"#;

        let config: ConfigFile = toml::from_str(toml).unwrap();

        // `spacetimedb::config::ConfigFile` doesn't implement `PartialEq`,
        // so check `common` in a pedestrian way.
        assert_eq!(&config.common.logs.directives, &["banana_shake=strawberry"]);
        assert!(config.common.certificate_authority.is_none());
        assert_eq!(config.common.wasm.procedure_instance_pool_size.get(), 4);
        assert_eq!(config.common.v8.procedure_instance_pool_size.get(), 3);
        assert_eq!(config.common.v8.heap_policy.heap_check_request_interval, None);
        assert_eq!(
            config.common.v8.heap_policy.heap_check_time_interval,
            Some(Duration::from_secs(45))
        );
        assert_eq!(config.common.v8.heap_policy.heap_gc_trigger_fraction, 0.6);
        assert_eq!(config.common.v8.heap_policy.heap_retire_fraction, 0.8);
        assert_eq!(config.common.v8.heap_policy.heap_limit_bytes, 128 * 1024 * 1024);
        assert_eq!(config.commitlog.log_format_version, Some(1));
        assert_eq!(
            config.commitlog.max_segment_size.map(|val| val.get()),
            Some(1024 * 1024)
        );
        assert_eq!(
            config.commitlog.offset_index_interval_bytes.map(|val| val.get()),
            Some(8192)
        );
        assert_eq!(config.commitlog.offset_index_require_segment_fsync, Some(false));
        assert_eq!(config.commitlog.preallocate_segments, Some(true));
        assert_eq!(
            config.commitlog.write_buffer_size.map(|val| val.get()),
            Some(128 * 1024)
        );

        assert_eq!(
            config.websocket,
            WebSocketOptions {
                idle_timeout: Duration::from_secs(60),
                close_handshake_timeout: Duration::from_millis(500),
                ..<_>::default()
            }
        );
    }

    #[test]
    fn commitlog_options_accept_aliases() {
        let toml = r#"
            [commitlog]
            offset-interval-bytes = 16384
            offset-index-require-fsync = true
"#;

        let config: ConfigFile = toml::from_str(toml).unwrap();
        assert_eq!(
            config.commitlog.offset_index_interval_bytes.map(|val| val.get()),
            Some(16 * 1024)
        );
        assert_eq!(config.commitlog.offset_index_require_segment_fsync, Some(true));
    }

    #[test]
    fn parse_mirror_spec_happy_path() {
        let spec = parse_mirror_spec("wss://ea.example/bitcraft-live-global").unwrap();
        assert_eq!(spec.database, "bitcraft-live-global");
        assert_eq!(spec.upstream.as_str(), "wss://ea.example/");
    }

    #[test]
    fn parse_mirror_spec_path_prefix() {
        let spec = parse_mirror_spec("https://other.host:443/prefix/bitcraft-live-7").unwrap();
        assert_eq!(spec.database, "bitcraft-live-7");
        assert_eq!(spec.upstream.as_str(), "https://other.host/prefix/");
    }

    #[test]
    fn parse_mirror_spec_missing_database() {
        assert!(parse_mirror_spec("wss://ea.example/").is_err());
        assert!(parse_mirror_spec("wss://ea.example").is_err());
    }

    #[test]
    fn parse_mirror_specs_rejects_duplicate_database() {
        let raw = vec![
            "wss://a.example/bitcraft-live-1".to_string(),
            "wss://b.example/bitcraft-live-1".to_string(),
        ];
        let err = parse_mirror_specs(&raw).unwrap_err().to_string();
        assert!(err.contains("duplicate"), "{err}");
        assert!(err.contains("bitcraft-live-1"), "{err}");
    }

    #[test]
    fn parse_mirror_specs_accepts_distinct_databases() {
        let raw = vec![
            "wss://a.example/bitcraft-live-global".to_string(),
            "wss://a.example/bitcraft-live-1".to_string(),
        ];
        let specs = parse_mirror_specs(&raw).unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].database, "bitcraft-live-global");
        assert_eq!(specs[1].database, "bitcraft-live-1");
    }
}
