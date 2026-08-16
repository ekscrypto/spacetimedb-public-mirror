# Multi-mirror starvation analysis (single instance, N upstreams)

> Analysis date: 2026-08-16. Line references are against fork HEAD `99972d1dd`
> ("Fix silent live stalls: cold-reset on reconnect and probe upstream liveness").
>
> Question investigated: when one `spacetimedb-standalone --public-mirror-v1`
> process mirrors multiple upstream `bitcraft-live-*` databases, WebSocket
> traffic starves on **both** the downstream-client side and the upstream side.
> Running one process per upstream works fine (current production topology).
> Was the cause in our code, or in a third-party dependency (reqwest et al.)?

## Verdict

**Our code.** No third-party WebSocket or HTTP client library is implicated.
Two fork-introduced defects cause the starvation, amplified by one
intended-and-correct guard:

1. **Defect:** a process-wide client gate that requires *every* mirror to be
   `Live` before any client is accepted — so one syncing or flapping mirror
   503s every region.
2. **Defect:** a cold-reset reconnect loop that trips that gate on any
   session error — so at N mirrors the gate is almost never satisfied and
   the mirrors serially re-seed forever.
3. **Intended guard (keep):** the shared subscribe semaphore pinned at one
   slot, so only one mirror at a time runs its ~8 GB transient sync — OOM
   protection on the 64 GB host. Working as designed; it merely stretches
   the window during which defect 1 blocks every client, which is why the
   defects surfaced so sharply in multi-mirror mode.

## Exonerated suspects

- **reqwest 0.12.24** — never touches the mirror WebSocket path. Its only
  mirror usage is a one-shot `reqwest::get` schema fetch at bootstrap
  (`crates/public-mirror/src/schema.rs:54`; called once per mirror from
  `start.rs:731`, not on reconnects). No shared `reqwest::Client`, no
  connection pool, and no pool tuning exist anywhere in the mirror path.
  (The reqwest 0.11.27 entry in `Cargo.lock` belongs to the JWKS test-only
  path and is unused here.) Minor nit only: the schema fetch has no timeout —
  irrelevant to the observed starvation.
- **tokio-tungstenite 0.27 / tungstenite 0.27** — each mirror gets its own
  dedicated `TcpStream` (`crates/public-mirror/src/upstream.rs:259-293`), kept
  deliberately un-split for the whole session so tungstenite auto-Pongs flush
  during multi-GiB fragmented seed reads. No state is shared between mirrors.
- **tokio 1.48.0** — one shared multi-thread runtime
  (`crates/standalone/src/main.rs:74-84`), default worker count (#CPUs),
  default 512-thread blocking pool. Shared, yes, but contention here is an
  amplifier (below), not the root cause.

## Mechanism 1 — process-wide client gate (client-side starvation)

`crates/client-api/src/routes/subscribe.rs:152-176` rejects **every**
downstream WebSocket upgrade with 503 unless `public_mirror_accepts_clients()`
passes, and that function (`crates/client-api/src/routes/mirrors.rs:85-88`)
requires **all** mirrors in the process to be simultaneously `Live`:

```rust
pub fn public_mirror_accepts_clients(statuses: &MirrorsResponse) -> bool {
    let mirrors = &statuses.mirrors;
    mirrors.is_empty() || mirrors.iter().all(|m| m.connectivity == MirrorConnectivity::Live)
}
```

With N mirrors:

- At startup, **zero clients are accepted for any database** — including
  databases whose mirrors are already fully live — until the *last* mirror
  finishes seeding.
- Any single mirror's transient disconnect 503s new connections to *every*
  region in the process.

Introduced by `49dde0bcb` ("reject WS clients until mirrors live + skip seed
broadcast"): the gate exists so seed apply can skip subscription eval /
broadcast with no risk of clients missing updates. That invariant is sound
per-database; checking *all* mirrors is what couples the regions together.

> **Fixed 2026-08-16:** replaced by `public_mirror_accepts_clients_for`
> (per-database gating keyed on the mirror's deterministic
> `DatabaseIdentity`); see Recommendations §1.

## Mechanism 2 — subscribe semaphore, default 1 (intended OOM guard)

`crates/standalone/src/subcommands/start.rs:360` builds one process-wide gate:

```rust
let subscribe_gate = Arc::new(tokio::sync::Semaphore::new(mirror_subscribe_concurrency)); // default 1
```

Each mirror acquires a slot before connecting and holds it through every
table's wire-seed download (`crates/public-mirror/src/runtime.rs:249-266`,
`upstream.rs` gate handling).

**This gate is intended and must stay.** A database sync requires roughly
8 GB of transient memory (wire-seed buffering + decode + apply); with 14
mirrors on a 64 GB host, concurrent syncs alone could exhaust RAM and crash
the process. The gate serializes syncing — once a mirror goes live, the next
one proceeds — and is the primary OOM protection for single-instance
operation. It also keeps a fleet-wide upstream blip from stampeding all
regions through re-seed at once.

Its role in the observed starvation is only to **stretch the exposure window**
of Mechanism 1: while one mirror syncs (potentially hours with multi-GiB
tables; commit `a8daed832` records 1.26 GiB tables), the remaining mirrors
sit in `waiting` with no upstream socket at all — which externally reads as
"upstream-side starvation" — and Mechanism 1 503s every client for every
region the entire time. With per-database gating (see Recommendations),
serial syncing costs only per-region availability latency, never client
starvation. Two second-order effects remain relevant to recovery time:

- Every reconnect cold-reset re-enters the gate for a full re-seed, so a
  flapping mirror's recovery queues behind any in-flight sync.
- Each re-seed re-incurs the ~8 GB transient spike, so the memory budget must
  assume one sync in flight at steady state, not just at cold start.

## Mechanism 3 — cold-reset reconnect cascade (why it never recovers)

The reconnect loop (`crates/public-mirror/src/runtime.rs:181-246`, from HEAD
commit `99972d1dd`) treats **any** session end as a full cold reset:

- triggers: 30 s liveness-probe timeout (`upstream.rs:72-76, 563-582`),
  5-minute subscribe stall (`upstream.rs:81-86`), 1 GiB decoded live-backlog
  cap (`upstream.rs:104, 883-899`), subscription errors, socket errors;
- sequence: `status.set_disconnected()` **first** — the comment at
  `runtime.rs:210-213` says this is so `public_mirror_accepts_clients`
  rejects new WS — which flips Mechanism 1 off for the whole process; then
  kick that database's subscribers and truncate its tables
  (`reset_mirror_for_reconnect`, `crates/core/src/host/module_host.rs:1897-1934`);
  then backoff (1 s → 30 s); then re-acquire the single subscribe slot
  (Mechanism 2) and re-download the entire seed before going `Live` again.

With N mirrors, the probability that at least one is non-live at any moment
approaches 1, so clients are almost never accepted and the mirrors serially
re-seed forever. This is precisely the observed "starvation on both the
client and upstream sides." One-instance-per-upstream contains each cascade
to a single region, which is why the per-instance fleet behaves acceptably.

## Amplifiers

These do not cause the starvation by themselves but feed Mechanism 3 and
worsen under N mirrors:

- **Live frames decode inline on the shared Tokio runtime.**
  `handle_background_frame` → `decode_server_message`
  (`crates/public-mirror/src/upstream.rs:586-591, 1244-1265`) does whole-frame
  brotli/gzip decompression + BSATN decode + delete-row `ProductValue::decode`
  synchronously on Tokio workers. The 256 KiB `spawn_blocking` offload
  (`OFFLOAD_DECODE_BYTES`, `upstream.rs:97, 687-691, 725-752`) is consulted
  **only** in `await_seed`, never in the live loop. N mirrors' decode load
  competes with every client WS send/recv loop on the same workers; ping and
  probe timers lag, producing more probe/stall timeouts → more cold resets.
  (The Linux `nice 5` mitigation covers mirror DB threads and offloaded seed
  decodes only, not live decode on Tokio workers — and is a no-op on macOS.)
- **Process-global rayon.** Client payloads ≥ 512 KiB are compressed via
  `spawn_rayon` (`subscribe.rs:1490, 1890-1896`) and initial subscription
  snapshots run `execute_plans` → `par_iter` (`crates/core/src/subscription/mod.rs:177-191`),
  all on one shared pool across all mirrors' clients.
- **Forced in-memory storage × N databases** (`start.rs:236-245`). One process
  holds all regions' multi-GiB resident state, and each in-flight sync adds
  ~8 GB of transient memory on a 64 GB host — the reason the one-slot
  subscribe gate (Mechanism 2) exists. The trial unit was additionally
  deployed with `MemoryHigh=8G` and a RAM guard that stops it if
  `MemAvailable` drops below 1 GiB; that guard remains essential for any
  single-instance deployment. Swap pressure slows every thread (including
  WS I/O) → timeouts → cascade.
- **Unbounded `deferred` frame buffer** during offloaded seed decodes
  (`upstream.rs:511-517, 745-749`) bypasses live-byte accounting until
  replay, so a burst can overshoot the 1 GiB cap and enter the cascade.

## Production evidence

- Current topology (from deploy artifacts; not re-confirmed over SSH during
  this analysis): `bitcraft-relay/tools/public-mirror@.service` is a systemd
  **template** unit — one process per upstream database on loopback
  `:3000+regionID`, coordinator-sequenced one at a time; the 14 production
  databases are listed in `bitcraft-relay/tools/public-mirror.instances`.
- The failed experiment survives as `bitcraft-relay/tools/public-mirror-trial.service`:
  a single process with two `--mirror` flags (`bitcraft-live-7` and
  `bitcraft-live-8`) on `127.0.0.1:3030`, plus
  `public-mirror-trial-ram-guard.service`.
- Git history: the fork's 29 commits (2026-07-28 → 07-31) are a four-day
  starvation-fix arc — inline applies stalling the socket (`a8daed832`),
  CPU-contention niceness (`e513d70f0`), per-DB executor threads (`4b9d8c2b0`),
  client gating (`49dde0bcb`), cold-reset + probe (`99972d1dd`). Every fix
  targeted single-mirror pain; the multi-mirror coupling (global gate +
  serial slot) was never revisited.

## Recommendations for reliable single-instance operation

Not implemented as of this writing; recorded for the future effort — except
item 1, implemented 2026-08-16.

1. **Per-database client gating — IMPLEMENTED 2026-08-16.** In the WS
   upgrade handler, resolve the target database first, then admit clients
   based on *that* mirror's status only (`public_mirror_accepts_clients_for`,
   matching on the mirror's deterministic `DatabaseIdentity` carried in the
   status registry/snapshot; databases without a mirror entry are accepted).
   Preserves the original invariant ("seed apply may skip subscription eval
   only while no client can be watching this DB") per-database. Existing clients of healthy regions keep
   their fan-out (already per-DB) and only the flapping region's subscribers
   get kicked. This makes the intended one-at-a-time syncing fully compatible
   with serving clients: regions come up incrementally, and each is usable
   the moment its own mirror goes live.
2. **Offload large live-frame decodes** like seeds already are: apply the
   `OFFLOAD_DECODE_BYTES` + deferred-frames ordering mechanism (exists in
   `await_seed`) to the live loop, on the niced blocking pool. Protects ping /
   probe timers under load and reduces cascade triggers.
3. **Keep the subscribe gate at 1.** Serial syncing is the OOM protection
   (~8 GB transient per sync, 64 GB host, 14 databases) and is fully
   compatible with reliable single-instance operation once clients are gated
   per database — it delays only the availability of not-yet-live regions.
   Do not raise `--mirror-subscribe-concurrency` without redoing the memory
   math (each extra slot is another ~8 GB transient). Optional future
   refinement: make the gate memory-aware (grant a slot only when
   `MemAvailable` exceeds a threshold) instead of fixed-count.
4. **Validate the steady-state memory budget.** Single-instance viability on
   the 64 GB host requires: sum of all 14 databases' resident (post-seed)
   footprints + one ~8 GB sync transient + runtime overhead, comfortably
   under 64 GB — including during reconnect re-seeds, which re-incur the
   transient. Measure per-instance RSS from the current `public-mirror@*`
   units and keep the RAM guard deployed.
5. **Verify via the existing isolated trial path.**
   `bitcraft-relay/tools/public-mirror-trial-deploy.sh` (regions 7+8 on
   `:3030`, no production impact) is the right harness: confirm clients of a
   live region are accepted while the other region seeds/reconnects, watch
   `/v1/mirrors`, ping latencies, backlog growth, and RSS.

## Quick reference

| Item | Where |
|------|-------|
| All-mirrors-live gate | `crates/client-api/src/routes/mirrors.rs:85-88` |
| Gate checked on WS upgrade | `crates/client-api/src/routes/subscribe.rs:152-176` |
| Subscribe semaphore (default 1) | `crates/standalone/src/subcommands/start.rs:360`, CLI at `start.rs:163-173` |
| Slot acquire/hold | `crates/public-mirror/src/runtime.rs:249-266` |
| Cold-reset reconnect loop | `crates/public-mirror/src/runtime.rs:181-246` |
| Live backlog cap (1 GiB) | `crates/public-mirror/src/upstream.rs:104, 883-899` |
| Inline live decode | `crates/public-mirror/src/upstream.rs:586-591, 1244-1265` |
| Seed-only decode offload | `crates/public-mirror/src/upstream.rs:97, 687-691, 725-752` |
| Upstream WS handshake (per-mirror TcpStream) | `crates/public-mirror/src/upstream.rs:259-293` |
| reqwest schema fetch | `crates/public-mirror/src/schema.rs:54` |
| Kick subscribers / truncate on reset | `crates/core/src/host/module_host.rs:1897-1934` |
| Per-instance systemd template | `bitcraft-relay/tools/public-mirror@.service` |
| Production instance list | `bitcraft-relay/tools/public-mirror.instances` |
| Failed 2-mirror experiment | `bitcraft-relay/tools/public-mirror-trial.service` |
