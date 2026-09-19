# How policy enforcement works

## Contract definition and engine behavior

A JSON contract names one dataset and defines its binding, allowed purposes,
exposed columns, and optional filters or masks. The contract is data. It does
not execute a query or protect a table by itself.

The engine evaluates that definition against the supplied `Caller`. The
resulting `ResolvedPolicy` contains an allow or deny decision plus the
operations to apply. The engine denies the query during planning or builds a
governed table provider. When DataFusion scans that provider, peQL executes
the row filter, masks, and optional noise operator in the physical plan.

## Caller context

`Caller` contains an ID, tenant, purpose, tier, and classification. The JSON
source currently uses the tenant and purpose to decide the view. An embedding
application supplies these values; peQL does not authenticate a user or verify
that a caller's claimed identity is genuine. The application must derive
`Caller` from its trusted authentication context.

For JSON contracts, a non-empty `purposes` list rejects a purpose outside the
list for both owners and non-owners. An owner tenant receives unmasked,
unfiltered values, but still sees only columns exposed by `columns`.

## Resolution and binding

Two traits keep policy evaluation separate from finding bytes:

| Trait | Returns | Included implementation |
| --- | --- | --- |
| `ContractSource` | `ResolvedPolicy` for a dataset and caller | `JsonContractSource` |
| `BindingResolver` | Raw DataFusion `TableProvider` for a dataset | Local Parquet loader in `JsonContractSource` |

The high-level `Engine` creates a fresh DataFusion session for each query. Its
catalog resolves a table name lazily. It checks the policy decision before
asking the binding resolver for the raw table. The standalone loader reads the
entire Parquet file into a `MemTable`; it does not stream large files.

## What happens during a scan

`ContractTableProvider` scans the raw table with all columns available. This
allows a row filter to use a column omitted from the SQL `SELECT` list. The
provider then builds this stack:

```text
raw scan
  → ScanMetricsExec
  → ContractApprovedExec
  → RowFilterExec
  → MaskingExec
  → LaplaceNoiseExec (if configured)
  → contract column projection
  → query projection and limit
```

The provider reports the schema *after* masking and contract projection to
DataFusion. String-producing masks on numeric or temporal fields therefore
change those fields to Arrow `Utf8` in the query's planned schema.

The high-level engine removes DataFusion's generic physical
`ProjectionPushdown` rule for these scans because it can move a projection
past a type-changing mask. The provider applies projections after enforcement.

## Graph queries

Graph table functions use the same caller-bound contract resolution. peQL
loads and verifies a graph snapshot, evaluates node and edge predicates into
visibility masks, then traverses only visible structure. A filtered node
cannot appear in output or act as an intermediate hop. Output masking uses
the physical masking operator. See {doc}`GRAPH-QUERY` for exact function
signatures and limits.
