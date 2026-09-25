# parcel and peQL

parcel is the language: it parses a contract, checks it against the data's schema, and compiles
it. peQL is the runtime: it stores what parcel compiled, writes data under it, and answers
queries through it. Nothing is implemented on both sides.

| parcel | peQL |
| --- | --- |
| The contract language (CEL rules, seven operations) and its checker | Contract store, versions, publication |
| The compiler: views' expressions, the validation plan, the write plan | Writing data, manifests, stored flags |
| `parcel-runtime`: the `Caller`, the CEL interpreter, parameter binding, `decide` and `unless` evaluation, validation over any table, shape rewrites, WebAssembly loading, bundles, SQL and Substrait export | Resolving guarantees against manifests, building views, the gate, charging budgets, the envelope, the audit log, the cache |
| `parcel check`: a contract against sample data, in memory | `peql write`, `query`, `validate`, `describe`, `publish` |

## The handoff

A **bundle** (`parcel compile -o contract.parcel.json`) is what crosses from one to the other.
It holds the contract document and the documents it inherits from, the data's schema, the
tenant functions it is pinned to, the three compiled artifacts, and the compilation hash.

peQL accepts a bundle only after recompiling it and getting the same hash, so it runs exactly
what parcel compiled or nothing. On the Griot platform, bundles also carry the contract
authority's signature ({doc}`platform`).

Both projects build on the same DataFusion version, and move to a new one together, parcel
first.
