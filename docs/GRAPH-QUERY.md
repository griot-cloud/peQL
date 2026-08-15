# GriotQL Graph Query — Design & Implementation Spec (2.0)

**Status:** design locked, implementation pending · **Requirements:** G01 (bundle) + G02 (query) · **This doc:** how G02 lands on GriotQL's real architecture and governance.

This is the single grounding artifact for the graph capability. G01 and G02 are
the *requirements* (bundle schema, R1–R11, acceptance tests T1–T10); this doc is
the *implementation contract* — the governance model, the mapping onto the code
in `src/`, the module plan, and the test matrix. Build against this.

---

## 1. What we're building

Six graph **table functions**, callable in ordinary SQL, each returning an
ordinary table, over a compiled process-graph snapshot:

`graph_node` · `graph_neighbors` · `graph_edges` · `graph_subtree` · `graph_path` · `graph_reachable`

plus **relational access** to the graph's node/edge tables (`graph_nodes(ref)`,
`graph_edges(ref)`). A graph is a **contract-bound dataset** — addressed and
governed exactly like a tabular dataset. The tagline: *naming a graph in SQL
executes the governing contract; then traversal is array arithmetic.*

## 2. The input: the snapshot bundle (G01 §9 — frozen contract)

```
process-graphs/{slug}/v{n}/
  nodes.parquet       # attrs + CSR columns: pos, parent_pos, call_ref_pos, out_start/out_count, in_start/in_count
  edges.parquet       # sorted by (src_pos, edge_type, dst_pos)   ← forward adjacency
  edges_rev.parquet   # sorted by (dst_pos, edge_type, src_pos)   ← reverse adjacency
  manifest.json       # bundle_format, graph_id/slug/kind, snapshot_version, counts, edge_types[], per-file sha256, cert ref
  certificate.gdcpc.signed
```

The compiler already baked the graph index into the file layout (CSR —
compressed sparse row). "Neighbors of node *p*" = read `edges[out_start..out_start+out_count)`.
No query-time index build. Invariants I1–I8 (dense positions, slices exactly
cover, no dangling refs, `contains`-forest = `parent_pos`, immutable, cert binds
digest) are compiler-guaranteed; the engine **verifies** digests + `bundle_format`
on load and otherwise relies on them. Positions are snapshot-scoped and MUST NOT
be returned as durable identifiers (I6) — ULIDs/names are the durable keys.

## 3. The governance model (the heart)

**One `ResolvedPolicy`, authored over node/edge *columns*, governs two surfaces.**
The policy is the same shape we already have (`src/policy.rs`): a decision plus
column masks plus a filter — reinterpreted for graphs.

### 3.1 Two surfaces

| Surface | What it is | Enforcement |
|---|---|---|
| **Relational** (`graph_nodes`/`graph_edges` scans, R11) | literally a `ContractTableProvider` over `nodes.parquet` / `edges.parquet` | **the existing operator stack, unchanged.** `row_filter` → `RowFilterExec`; `masks` → `MaskingExec`. Free. |
| **Traversal** (the six functions) | output is a table, but produced by *walking structure* | the **same** policy, compiled to a **node-visibility bitmask + output masker**; the walk consults the bitmask as a **wall**. |

### 3.2 The one real rule: hidden = wall, not erased

A policy that filters a node out must make that node **non-existent AND
non-traversable** for the caller. `graph_path(A,B)` whose only route crosses a
filtered node returns **no path**; `graph_reachable(A)` excludes anything reachable
only through it; edges incident to it vanish. Filtering the *output row* while
still routing paths *through* the node leaks topology — the exact bug G02's **T7**
test exists to catch. Mechanism: evaluate the node predicate once at load into
`visible: BitVec[N]`; traversal skips invisible nodes as neighbors **and never
expands through them**.

### 3.3 Three governance levers (all authored over columns)

| Lever | Authored as | Table analogue? | Graph consequence |
|---|---|---|---|
| **Column mask** | `masks: { owner: redact, system_ref: redact, condition: redact, … }` | ✓ | value hidden in every output (functions + relational) |
| **Node visibility** | `node_filter: <predicate over node columns>` (v1: `kind`/`tags`/`owner`/subtree membership) | ✓ (row_filter) | node is a **wall** — absent + non-traversable |
| **Edge visibility** | `edge_filter: <predicate over edge columns, e.g. edge_type>` | ✗ (new) | relationship hidden even between two visible nodes |

Edge visibility is the **only genuinely new governance concept** graphs add —
because edges are first-class structure, you can hide a *relationship* ("hide
`depends_on`") without hiding either endpoint. R4's "edges incident to a filtered
node are invisible" is the *derived* case; `edge_filter` is the *authored* case.

### 3.4 What to mask vs. hide vs. leave (default posture)

- **Never mask, filter instead:** `name` (the agent's navigation key — masking blinds while admitting existence). Sensitive step ⇒ remove the node.
- **Mask (node exists, see less):** `owner`, `system_ref`, edge `condition`, optionally `data_refs`, `description`, `updated_by`.
- **Hide (wall):** whole sensitive sub-processes, `system` nodes, or dependency edges — via `node_filter` / `edge_filter`.
- **Leave visible:** `kind`, `executed_by`, `status`, `edge_type` — the analytical point; also the natural filter axes.

**Recommended default posture for outside tenants: hide the structurally
sensitive, mask the attribute-sensitive.** (Open decision D1, §9.)

### 3.5 "In the framework, not beside it"

Guaranteed structurally, not by discipline: the **governed snapshot handle is
produced only through contract resolution** (like `ContractTableProvider` is
produced only through the catalog), and *both* the functions and the relational
scans hang off it. There is no way to obtain graph bytes without the policy
having run — the engine's non-negotiable "no un-governed path" property holds for
graphs by construction.

## 4. Architecture — mapping onto `src/`

### 4.1 Reused unchanged

| Existing | Role for graphs |
|---|---|
| `ContractSource::resolve(dataset, caller) → ResolvedPolicy` (`contract_source.rs`) | **unchanged** — a graph resolves through the same seam, contract-first |
| `ResolvedPolicy` (`policy.rs`) | carries `masks` + `node_filter` (+ new `edge_filter`) + decision |
| `MaskAction`, `MaskingExec`, `RowFilterExec`, `ContractApprovedExec` (`physical/`) | the relational surface + output masking |
| `GriotEngine::query(sql, caller)` (`engine.rs`) | **unchanged** — graph functions just appear in SQL |
| `PlatformBundleSource` + p256 verify (`platform/`) | the GDCP certificate verify reuses this path |
| PyO3 `Engine.query` (Arrow IPC → pyarrow) | **unchanged** (R10) — results are ordinary tables |

### 4.2 New components

- **`graph::bundle`** — load `nodes/edges/edges_rev.parquet` + `manifest.json`; verify `bundle_format` + per-file sha256 (R6); build Arrow-backed CSR arrays + `name→pos` / `ulid→pos` indexes.
- **`graph::snapshot::GovernedGraphSnapshot`** — the graph analogue of `ContractTableProvider`: holds the CSR arrays, the `visible: BitVec` compiled from `node_filter`, the edge-visibility mask from `edge_filter`, and the output masker from `masks`. Produced **only** via `ContractSource` + bundle load. Exposes `node()`, `neighbors()`, `edges()`, `subtree()`, `path()`, `reachable()` returning governed `RecordBatch`es.
- **`graph::session_cache`** — first call per (snapshot, **policy fingerprint**) loads + verifies + builds the governed snapshot; later calls reuse it. Two callers with different policies never share a structure; session end discards (R5).
- **`graph::functions`** — the six + two relational, registered as DataFusion **UDTFs** (`register_udtf`), **bound to the caller per session** (like the caller-bound catalog) so each resolves the graph contract for that caller. Each UDTF: resolve `graph_ref` → get/load governed snapshot from cache → traverse → return a `MemTable` (composable, R3).

### 4.3 Caller threading

`TableFunctionImpl::call` has no caller identity (as `SchemaProvider::table`
didn't). Solution mirrors the catalog: `GriotEngine::query` registers the graph
UDTFs into the per-query, caller-bound session. No global state.

## 5. The six functions (condensed from G02 §7)

Common: `graph_ref` (pinned/unpinned; unpinned discloses resolved `snapshot_version`,
R7); `node` accepts name or ULID (ULID-shaped tried as id first); `edge_types`
defaults to all authored types — `contains`/`contains_ref` excluded except in
`graph_subtree`; `direction ∈ {out,in,both}`. Deterministic order per function.
Caps: `max_depth`=12, `max_results`=10 000; cycles terminate (visited-set);
truncation sets a `truncated` marker.

| Function | Returns | Order |
|---|---|---|
| `graph_node(ref, node)` | one row: full node attrs | — |
| `graph_neighbors(ref, node, direction, edge_types, include_edge)` | one row per (neighbor, edge): node cols + edge cols + direction | direction, edge_type, neighbor pos |
| `graph_edges(ref, node?, direction, edge_types)` | edges (of a node, or all): full edge schema + resolved src/dst names | by row |
| `graph_subtree(ref, node, max_depth, follow_call_refs)` | anchor + descendants via `contains` (+`contains_ref` if opted in): node cols + depth + parent_pos | depth-first, children by pos |
| `graph_path(ref, from, to, direction, edge_types, max_depth)` | one shortest path (BFS, tie-break lowest neighbor pos): one row per hop | step |
| `graph_reachable(ref, node, direction, edge_types, max_depth)` | distinct reachable nodes: node cols + min_depth + first_edge_type | min_depth, pos |

## 6. The graph contract (extends the JSON contract format)

```json
{
  "contract_id": "zijani_ops_graph",
  "dataset": "process-graphs/zijani-operations/v7",
  "binding": { "graph_snapshot": "lakehouse://process-graphs/zijani-operations/v7/" },
  "owner_tenant": "zijani",
  "purposes": ["process_analysis"],
  "masks":       { "owner": "redact", "system_ref": "redact", "condition": "redact" },
  "node_filter": "kind != 'system'",
  "edge_filter": "edge_type != 'depends_on'"
}
```

- Owner tenant → allow-all (raw graph). Everyone else → masks + walls.
- New vs. tabular contracts: `binding.graph_snapshot` (a bundle dir, not one Parquet); `edge_filter` (no tabular analogue). `masks`/`node_filter` are the tabular `column_masks`/`row_filter` reinterpreted over node columns. Platform path: same policy from a T03/GDCP source.

## 7. Fixtures (build-time prerequisite)

G01's compiler is not built yet, so we **generate a spec-compliant bundle** to
build against (G01 §12 intends exactly this):

- A **fixture generator** (Rust or PyArrow) emitting byte-compliant `nodes/edges/edges_rev.parquet` + `manifest.json` + a signed cert, honoring I1–I8.
- Two fixtures: a **golden `zijani-operations`** graph, and a **synthetic 12-node** graph with every edge type, a shared subprocess (`call_ref`), and a deliberate `depends_on` cycle (for visited-set + tie-break tests).
- When G01's real compiler lands, swap the fixture for its golden bundle and match bytes.

## 8. Phased build + R→T test matrix

| Phase | Deliverable | Covers |
|---|---|---|
| P0 | Fixture generator + golden/synthetic bundles | T8/T9 substrate |
| P1 | `graph::bundle` loader + verification (digests, format, cert) | R6 → **T6** |
| P2 | `GovernedGraphSnapshot`: CSR + visibility bitmask (`node_filter`) + edge mask + output masker; session cache | R4/R5 → **T5** |
| P3 | The six functions as caller-bound UDTFs + traversal algorithms (caps, visited-set, determinism) | R2/R7/R8 → **T3/T8/T9** |
| P4 | **Governed traversal + the topology-leak test** (the correctness gate) | R4 → **T7** |
| P5 | SQL composability + relational scans (R11) + Python parity | R3/R10/R11 → **T4/T10** |
| P6 | Contract-first resolution (pinned/unpinned, deny=not-found) + platform cert path | R1/R9 → **T1/T2** |

Every phase is green-gated (build/clippy/fmt/test on default **and** `platform`),
consistent with the existing repo.

## 9. Open decisions (confirm before/early in build)

- **D1 — outside-tenant default posture:** mask-heavy (a redacted but complete map) vs. hide-heavy (a smaller map). *Recommendation:* hide structurally sensitive (sub-processes, system nodes, dependency edges), mask attribute-sensitive (owner, system_ref, condition).
- **D2 — edge visibility surface:** first-class `edge_filter` field vs. deriving edge visibility only from `node_filter`. *Recommendation:* first-class `edge_filter`, optional (derived-only if absent) — it's the one new lever and it's cheap.
- **D3 — DataFusion UDTF named args:** G02 shows `direction => 'in'`. If DF47 UDTFs don't accept named notation, functions use positional args (prototype this first).

## 10. Non-goals (G02 §10)

No graph query language (no Cypher/MATCH); no writes; no virtual graphs over live
tables (designed-for, not built — the opaque-surface rule, R9, is the whole
forward-compat investment); no large-graph frontier regime (v1 envelope
≤50k nodes / 200k edges; exceeding it fails with a clear error); no cross-graph
traversal (join across graphs relationally instead).
