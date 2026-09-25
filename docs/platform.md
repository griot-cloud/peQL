# The Griot platform

## Signed bundles from the contract authority (feature `platform`)

On the platform, contracts come from T03 as parcel bundles signed with ECDSA P-256.

```rust
let source = peql::platform::PlatformBundleSource::new("https://t03.internal")
    .with_verifying_key(key)
    .with_auth("Authorization", format!("Bearer {token}"));
source.register(&engine, "demo/users").await?;
```

`GET {base}/v1/contracts/{name}/bundle` returns a `SignedBundle`: the bundle, a DER signature,
and metadata (the bundle's digest, when it was signed, the key generation). The digest covers
the bundle's format, name, version, contract and compilation hashes, and every function module
it carries. The signature proves who issued the bundle; recompiling it to its compilation hash
proves what it contains. `SignedBundle::sign` is what the authority runs.

## The tenant engine and the worker pool

`K04DEngine` serves one tenant. `inject_contract_bundle` registers a bundle for that tenant (a
bundle for another tenant is refused). `register_memory_table`, `register_parquet_table` and
`register_lance_table` bind data to a registered contract; there are no raw tables. `query` takes
the caller and returns the rows and the envelope, and refuses results larger than
`max_result_rows`.

`LongRunningPoolManager` runs queries on a bounded set of workers that share one engine, with a
queue per worker, tenant affinity, and a drain on shutdown. With a signer, each result's envelope
is signed; `T05Client` signs over the T05 notary socket.

## Lance datasets (feature `lance`)

`LanceTableProvider::open_uri` reads a Lance dataset from a path or object-store URI;
`LanceTableProvider::open` reads one through the T04 storaged socket. Either is served under a
contract with `bind_table` (or `K04DEngine::register_lance_table`). Scans stream; projections,
limits and filters that cannot fail are passed to Lance, and DataFusion re-checks the filters.
Lance uses an older Arrow than peQL, so batches cross by Arrow IPC.

A Lance dataset is a directory of objects, so reads through storaged name the object:

| Opcode | Request adds | Response |
| --- | --- | --- |
| `0x30` read | `path` (optional; absent reads the asset itself) | as before |
| `0x31` stat | `path` (optional) | `size` (`content_type`, `format_version` optional) |
| `0x32` list | `prefix` | `{"objects": [{"path", "size"}]}` |

`0x32` and the `path` field are additions T04 serves for Lance assets; single-file assets are read
exactly as in 0.3.
