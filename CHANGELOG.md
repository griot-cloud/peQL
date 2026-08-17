# Changelog

All notable changes to this project are documented here. The format is loosely
based on [Keep a Changelog](https://keepachangelog.com/).

## [2.0.0] — graph query capability

GriotQL traverses compiled business-process graphs in plain SQL, governed by
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
  `CatalogProvider`), and `engine` (`GriotEngine`).
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

Initial standalone snapshot of the GriotQL query engine, **copied** out of the
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
