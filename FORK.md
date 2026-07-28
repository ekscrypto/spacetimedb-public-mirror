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
# Repeat --mirror for each upstream database; one process serves them all
# on a single --listen-addr (clients select by database name).
./target/release/spacetimedb-standalone start \
  --data-dir /tmp/public-mirror-data \
  --listen-addr 127.0.0.1:3001 \
  --jwt-pub-key-path /path/to/id_ecdsa.pub \
  --jwt-priv-key-path /path/to/id_ecdsa \
  --public-mirror-v1 \
  --mirror-token-file /path/to/.developer-token \
  --mirror wss://bitcraft-early-access.spacetimedb.com/bitcraft-live-global \
  --mirror wss://bitcraft-early-access.spacetimedb.com/bitcraft-live-1 \
  --mirror-table player_username_state \
  --mirror-table player_state
```

`--public-mirror-v1` forces in-memory storage. `CallReducer` / `CallProcedure` are
always rejected. Pass `--reject-one-off-query` to also reject `OneOffQuery`
(allowed by default). Each `--mirror` is `<upstream-url>/<database-name>`; token
and `--mirror-table` are shared across all mirrors. Token may also come from
`--mirror-token`, `BITCRAFT_TOKEN`, `MIRROR_TOKEN`, `RELAY_UPSTREAM_TOKEN`,
or `MIRROR_TOKEN_FILE`. Use
`--mirror-table` to limit the upstream subscribe set (default: all public user
tables). Each mirrored database runs on its own JobCores thread.

Clients connect to the local mirror by database name (`bitcraft-live-1`,
`bitcraft-live-global`, …) on the listen address, speaking `v1.bsatn.spacetimedb`.

Initial connect/subscribe is gated by `--mirror-subscribe-concurrency` (default
**1**): only that many mirrors may **set up mirroring** at once. A slot is
acquired before connecting and held for the entire setup phase — connect, every
table's wire seed, and every local seed apply — then released only when the
mirror goes live. Live mirrors never hold a slot; a mirror that disconnects
must reacquire one (after backoff) before reconnecting, and shows `waiting`
until it does.

**WebSocket servicing is never blocked on database work.** The socket loop only
reads, decodes, and enqueues; applies run from a FIFO queue whose in-flight job
(on the mirror's dedicated JobCores thread) is polled *concurrently* with
socket reads and Pings. A stuck or multi-minute insert costs queue depth (live
backlog is capped at 1 GiB decoded — beyond that the session errors and
reconnects instead of growing without bound), never the connection. Queued live
updates are applied in batches (one executor job per batch) to amortize the
cross-thread round trip, and whatever was already received is drained to the
database before a failed session returns. On Linux the mirror apply threads run
at niceness 5 so saturating seed inserts yield the CPU to socket tasks.

**Live invariant:** once a mirror reaches `live`, it does not touch the subscribe
gate again until disconnect/reconnect. Another mirror's subscribe or seed must
not block that database's table updates (dedicated JobCores thread per DB; large
frame decompress/decode runs on Tokio's blocking pool so shared async workers
stay free for live WebSocket tasks). It is OK for a *disconnected* mirror to
stay `waiting` until a subscribe slot opens.

Raise the concurrency only if you have evidence the upstream can absorb concurrent
large-shard wire seeds.

Per-mirror connectivity (waiting / connecting / subscribing / live / disconnected),
table sync progress, transaction counts, and reconnect ETA are exposed at
`GET /v1/mirrors` (JSON, unauthenticated — same posture as `/v1/metrics`).
While `subscribing`, the response also includes the current table name/phase,
socket bytes received since that subscribe started, and `last_byte_at` so you
can tell a slowly arriving seed from a hung connection. During `applying_seed`,
`current_table_seed_rows_applied` / `last_seed_apply_at` tick as rows are
inserted locally. Seed applies are **idempotent**: each table is truncated in
the same transaction before its snapshot rows are inserted, so a reconnect
re-seed converges instead of crash-looping on unique-constraint violations
against rows left over from the previous session (downstream subscribers only
see the net diff).

Large full-table seeds (e.g. `location_state`, 1 GiB+) are kept alive by
design:

- The client requests **Brotli** compression (`compression=Brotli`), cutting
  large seed snapshots ~7–10x on the wire. Whole-message and per-query-update
  brotli/gzip are both decompressed transparently.
- The subscribe timeout is **stall-based**: a subscribe only fails when the
  socket has been completely silent for 5 minutes (plus a generous 2 h absolute
  cap). A seed that is still trickling in is never killed mid-transfer.
- The upstream client leaves tungstenite message/frame caps unlimited, never
  splits the WebSocket (so auto-Pongs flush mid-reassembly), and keeps client
  Pings flowing during seed wait and apply.
- Seed rows are zero-copy slices of the decoded frame (one shared allocation),
  not per-row copies.
- If a session dies while subscribing a specific table, that table is
  subscribed **first** on the next attempt — the riskiest transfer happens
  while the connection is freshest instead of after re-seeding every other
  table.
- The post-connect `IdentityToken` wait is bounded (60 s) so a dead upstream
  cannot hold a subscribe-gate slot forever.

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