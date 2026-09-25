# Changelog

All notable changes to this project are documented here. The format is loosely
based on [Keep a Changelog](https://keepachangelog.com/).

## [0.4.0]: the runtime for parcel contracts

peQL now enforces contracts written in [parcel](https://github.com/griot-cloud/parcel) and has
no policy language of its own. parcel compiles each contract; peQL stores, writes, validates and
queries under it. Every 0.3 policy has a parcel equivalent ([migration guide](docs/migrating.md)).

**Enforcement**
- Contracts are views: a filter of `admit` rules and drop-level assertions, and a projection of
  exposed columns and transforms, with the caller bound as literals. The optimiser pushes the
  filter into the Parquet scan and prunes files and row groups. 0.3 read every file whole
  before filtering.
- A gate on every view. The engine refuses a plan in which contract data is read outside its
  gate, and the gate stops caller predicates that could fail from running on hidden rows.
- `decide`, `guarantee` (against manifests), `assert` (stored flags), and every shape:
  `sample`, `noise` at rows or aggregates, and `suppress` in every aggregate.
- Privacy budgets are charged once per query and only for queries that read the noised column.
  They are enforced by default and persist across restarts.
- The statement guard parses SQL instead of scanning keywords.

**Lifecycle**
- The write path (flags, clustering, partitions, bloom filters, contract hash in each file),
  manifests, validation with a data hash, and re-validation of every contract over shared files.
- A contract store with versions and publication to tenants or `public`. Contracts a caller may
  not see are indistinguishable from absent ones.
- Tenants' WebAssembly functions, stored by owner and callable only from their own contracts.
- parcel bundles as the handoff: registered only when they recompile to the same hash.

**Kept, rebuilt on the new engine**
- `K04DEngine`: tenant-scoped and governed. In 0.3 it ran SQL over raw registered tables with no
  enforcement.
- The worker pool, now sharing one engine and able to sign envelopes through T05.
- The result cache. Its key now covers the caller's bound context and the data, where 0.3's
  `(tenant, sql)` key could serve one caller's rows to another in the same tenant.
- Result formats (Arrow IPC, Parquet, JSON lines).
- Scan statistics and attestation hashes, now in every query's envelope.
- The platform adapter: signed parcel bundles from T03 in place of SQL templates and Rego.
- Lance: now streamed, with projections and safe filters passed down. Reads through storaged
  name each object, fixing 0.3's provider, which read the same bytes for every object and could
  not open a Lance dataset.
- Python bindings, with the same `Caller(id, purpose, tenant)` and new `write`, `validate`,
  `describe`, `publish`.
- A `peql` command line, released as binaries for Linux (x86_64, arm64), macOS (arm64, x86_64)
  and Windows (x64) with `install.sh` and `install.ps1` installers, and Python wheels for the
  same platforms.

**Removed**
- The JSON contract format, `ResolvedPolicy`, the optimiser rules and the row-filter, masking,
  contract-approved and Laplace-noise operators: parcel expresses these now.
- Graph table functions: graphs are two contracts traversed with recursive SQL
  ([docs](docs/graphs.md)).

**Mask changes**: `redact` is a fixed `***` whatever the length, `partial` fully masks values of
four characters or fewer, and the null mask gives a real null instead of an empty string.

DataFusion 55, Arrow 59, Rust 2024 edition; license Apache-2.0.

## [0.3.0] — graph queries, peQL naming, and documentation

This release follows `0.2.0`. The premature `v2.0.0` tag is withdrawn; the
graph work below is included in `0.3.0` instead. The Rust crate and Python
package are now named `peql`. The high-level Rust query type is `Engine`;
`K04DEngine` remains available as a separate lower-level Rust API. The
documentation is rebuilt with Sphinx and organized into learning, how-to,
reference, explanation, and contribution paths.

peQL traverses compiled business-process graphs in plain SQL, governed by
the same contract policy as tables.

- **Seven SQL table functions**: `graph_node`, `graph_neighbors`, `graph_edges`,
  `graph_subtree`, `graph_path`, `graph_reachable`, and the relational
  `graph_nodes` — each returns an ordinary table that composes with joins,
  CTEs and aggregation. Positional arguments; results disclose
  `snapshot_version` and (where capped) `truncated`.
- **Governed traversal**: a policy-filtered node is a **wall** — absent from
  every result *and* non-traversable (no path routes through it; nothing
  reachable only via it is reachable) — closing the topology-leak class. A
  graph-only `edge_filter` hides relationships between visible nodes. Column
  masks run through the real masking operators, byte-identical to tables.
  Deny and unknown-graph are byte-identical (no existence oracle); unknown
  nodes return near-miss suggestions.
- **Snapshot bundles** (G01 format): loaded from `nodes/edges/edges_rev.parquet`
  + `manifest.json` with mandatory sha256 + `bundle_format` verification and
  structural invariant checks; traversal runs on the precompiled CSR offset
  columns — no query-time index build. Per-engine cache keyed by
  (snapshot, policy fingerprint): bundles load once; different policies never
  share a governed structure. v1 envelope ≤50k nodes / 200k edges, enforced.
- **Contract format**: graph datasets bind via `binding.graph_snapshot`;
  policy via `masks`, `node_filter` (row_filter alias over node columns) and
  `edge_filter`. `ResolvedPolicy` gains `graph_edge_filter`.
- **Python parity**: the same graph SQL works through the wheel unchanged;
  list columns (`data_refs` …) round-trip to pyarrow.
- Verified end-to-end against the real compiled `zijani-operations` bundle
  (271 nodes / 562 edges): examples/graph_query.rs + 10 SQL E2E tests,
  33 traversal-algorithm tests, and bundle loader tests.
- Design + as-built spec: [`docs/GRAPH-QUERY.md`](docs/GRAPH-QUERY.md).

## [0.2.0] — contract resolution spine + platform adapter

Turned the engine from "mask a hand-fed batch" into "query a contract": you now
name a contract-bound dataset in SQL and get governed rows.

- **Contract resolution spine.** `SELECT … FROM "<dataset-uri>"` resolves the
  governing contract for the caller and the physical data location, then applies
  the contract's masking, row filtering and DP noise inside the query plan.
  New modules: `policy` (`ResolvedPolicy` — the engine-agnostic enforcement
  primitive), `contract_source` (`ContractSource` + `JsonContractSource`),
  `binding` (`BindingResolver` + local Parquet loader), `contract_table_provider`
  (`ContractTableProvider`), `catalog` (lazy DataFusion `SchemaProvider`/
  `CatalogProvider`), and `engine` (`Engine`).
- **Open-source path.** A simple JSON contract format + local Parquet — no
  services, no `protoc`. See `docs/CONTRACT-FORMAT.md`.
- **Platform adapter** (`--features platform`). `PlatformBundleSource` fetches a
  Griot Cloud T03 signed bundle over HTTP, verifies its ECDSA-P256 signature
  (canonical hashing mirrored from T03's `bundle-signer`), and maps it to the
  same `ResolvedPolicy`. Exercised offline against the real committed bundle
  fixture.
- New examples: `contract_query` (standalone) and `platform_bundle` (platform).
- Docs: `docs/USAGE.md`, `docs/CONTRACT-FORMAT.md`, `docs/ARCHITECTURE.md`.
- The pre-existing enforcement operators are reused unchanged.

## [0.1.0] — initial snapshot

Initial standalone snapshot of the peQL query engine, **copied** out of the
Griot Cloud platform monorepo.

- **Source:** `griot-cloud` @ commit `5b999ed0010aeb86ae703076798f035b9c0c9121`
  (path `zone-k/k04-workers/k04d-query-rs/`).
- Trimmed the cloud-only deployment glue (HTTP service wrapper, GCS/pgvector/
  Redis clients, container/build manifests) and the legacy gRPC server so the
  crate builds and runs standalone with only Rust + cargo.
- Pruned the now-unused dependencies; the default build needs no `protoc` and no
  system libraries.
- Kept the engine intact: the DataFusion optimizer rules and physical operators
  (contract check, row filter, column masking, differential-privacy noise,
  attestation), the sealed engine core, and the DDL guard.
- Added four runnable examples (`plain_sql`, `column_masking`, `row_filter`,
  `dp_noise`) that demonstrate governed output with zero external services.
- `lance` columnar support retained as an optional feature (off by default;
  requires `protoc`).

The engine's behaviour is unchanged from the source commit; this release is a
packaging + documentation pass, not a redesign.
