# spacetimedb-public-mirror

Unofficial public fork of [Clockwork Labs SpacetimeDB](https://github.com/clockworklabs/SpacetimeDB),
pinned at tag **v2.7.1**.

## Purpose

Adds `--public-mirror-v1`: an in-memory SpacetimeDB mode that mirrors a remote
**v1 BSATN** database and fans out committed `TransactionUpdate`s to local
subscribers **with the original upstream reducer call stack / provenance**.

SpacetimeDB WebSocket protocols include v1, v2, and v3. Full reducer provenance
(`reducer_call`, caller identity/connection, timestamp) exists on the wire
**only in v1**. This mode is intentionally v1-scoped.

## Not for upstream merge

Clockwork Labs is unlikely to accept this feature. This fork exists so the
relay fleet can eventually replace the capture → `relay_apply_*` → MetaRegistry
rewrite path with in-engine mirroring. It does **not** replace
`spacetimedb-relay` or `bitcraft-relay` today.

## License

SpacetimeDB is licensed under the Business Source License 1.1 (see
[`LICENSE.txt`](LICENSE.txt)). This fork redistributes the Licensed Work with
modifications under the same terms. Keep that file conspicuous on every copy.

BSL Additional Use Grant limits (e.g. production instance count / “Database
Service”) still apply to how you *run* the software — see the upstream license
text. This README is not legal advice.

## Relation to the BitCraft relay workspace

Intended sibling checkout layout:

```
relay-bitcraftsync-app/
├── spacetimedb-relay/          # production capture/rewrite relay
├── bitcraft-relay/             # BitCraft cache + fleet ops
└── spacetimedb-public-mirror/  # this fork (experimental)
```

Live validation target: BitCraft EA2 region 1 (`bitcraft-live-1`), which the
current fleet does not mirror.

## Quick start (mirror mode)

```sh
cargo build -p spacetimedb-standalone --release

# Prefer the fleet developer JWT (not the Unity PlayerPrefs player token).
# Multi-line token files are OK: the first eyJ… line is used.
./target/release/spacetimedb-standalone start \
  --data-dir /tmp/public-mirror-data \
  --listen-addr 127.0.0.1:3001 \
  --jwt-pub-key-path /path/to/id_ecdsa.pub \
  --jwt-priv-key-path /path/to/id_ecdsa \
  --public-mirror-v1 \
  --mirror-upstream wss://bitcraft-early-access.spacetimedb.com \
  --mirror-database bitcraft-live-1 \
  --mirror-token-file /path/to/.developer-token \
  --mirror-table player_username_state \
  --mirror-table player_state
```

`--public-mirror-v1` forces in-memory storage. `CallReducer` / `CallProcedure` are
always rejected. Pass `--reject-one-off-query` to also reject `OneOffQuery`
(allowed by default). Token may also come from `--mirror-token`, `BITCRAFT_TOKEN`,
`MIRROR_TOKEN`, or `MIRROR_TOKEN_FILE`. Use `--mirror-table` to limit the
upstream subscribe set (default: all public user tables).

Clients connect to the local mirror by database name (`bitcraft-live-1`) on the
listen address, speaking `v1.bsatn.spacetimedb`.

### Compatibility harness

```sh
cargo run -p spacetimedb-public-mirror-client --bin mirror-harness -- \
  --upstream wss://bitcraft-early-access.spacetimedb.com \
  --database bitcraft-live-1 \
  --token "$MIRROR_TOKEN" \
  --mirror-url ws://127.0.0.1:3001 \
  --table player_username_state \
  --seconds 30
```

Compares committed `TransactionUpdate` reducer name / request_id / caller and
row counts between upstream and the local mirror; prints `PASS` / `FAIL`.
Note: an unused region may have empty tables and little live traffic.