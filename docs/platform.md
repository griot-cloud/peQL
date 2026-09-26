# Serving peQL

peQL embeds in one process that owns the data: it opens the workspace, and everything reaches the
data through it. These parts let that process serve callers, prove its answers, and keep its data
in an object store.

## Flight SQL (feature `flight`)

`peql::flight::FlightSql` is a Flight SQL service over tonic. You supply the listener:
`serve_unix` and `serve_tcp` take one, and `serve` takes any stream of connections tonic can
accept (a vsock, for example). `into_service` gives the tonic service for a server of your own.

```rust
let engine = Arc::new(peql::Engine::open("/data")?);
let listener = tokio::net::UnixListener::bind("/run/peql.sock")?;
peql::flight::serve_unix(peql::flight::FlightSql::new(engine), listener).await?;
```

| Flight SQL | peQL |
| --- | --- |
| `GetFlightInfo`, `CreatePreparedStatement` | `Engine::check`: the statement guard, visibility, `decide` and `guarantee`. A refusal is returned here, before any scan, and audited. |
| `DoGet` | `Engine::query`. The first message's `app_metadata` is JSON: `{"envelope": …, "signature": …}`. |
| `DoPut` (bulk ingest) | `Engine::write` under the contract named by `table`, by the contract's owner. `replace` overwrites, `append` appends; `fail if exists` is refused, since a contract exists before its data. |

Every request names its caller in the `x-peql-caller` header: a `Caller` as base64 JSON
(`peql::flight::caller_header` makes one). The header is believed, so the listener must be
reachable only by whoever authenticated the caller. `FlightSql::for_caller(engine, caller)`
answers every request for one caller fixed when the service is built, and ignores the header.

A ticket is the SQL. `DoGet` checks everything again for its own caller, so a ticket grants
nothing by itself. Errors keep peQL's words: `PermissionDenied` for a refusal, `NotFound` for a
contract that does not exist or is not visible, `ResourceExhausted` for a spent budget.

## Signed envelopes

`Engine::with_signer` signs every answer's envelope through an `EnvelopeSigner`; the JWS comes
back as `QueryResult::signature`. With a signer configured, an envelope that cannot be signed
fails the query: no answer leaves without its certificate.

`peql::SocketSigner` is a signer at the other end of a socket (`unix(path)`, `tcp(addr)`, or
`with_connector` for any stream). One connection per envelope, one line each way:

```text
engine → signer   the envelope as JSON, then '\n'
signer → engine   {"jws": "<compact JWS>"}  or  {"error": "<why>"}, then '\n'
```

The envelope names the caller as the engine was told it. A signer that authenticated the caller
itself should bind what it knows rather than the envelope's claim.

## Signed bundles (feature `signed-bundle`)

A `SignedBundle` is a parcel bundle with its issuer's ECDSA P-256 signature. peQL verifies;
signing is the issuer's. `SignedBundle::register(&engine, &key)` checks the signature and then
registers the bundle, which recompiles it to its compilation hash. The signature proves who
issued the bundle; the recompilation proves what it contains. `signing_payload()` is the exact
byte string the issuer signs: a digest of the bundle's format, name, version, hashes and function
modules, with the key generation and the signing time.

## Bindings in an object store

`ObjectStoreParquet` serves bindings from a prefix of an object store the way `LocalParquet`
serves directories: a streaming listing table with hive partitions and pruning, written by the
same write path, with manifests beside the data.

```rust
let bindings = peql::ObjectStoreParquet::s3_from_env("s3://lake/tenant-a/")?; // feature `s3`
let engine = peql::Engine::open("/var/lib/peql")?.with_bindings(Arc::new(bindings));
```

A binding such as `s3://lake/tenant-a/orders/` must name the resolver's bucket; a relative one
(`orders/`) resolves under its prefix. Any `object_store` store works with
`ObjectStoreParquet::new(base, store)`.

## Lance datasets (feature `lance`)

`LanceTableProvider::open_uri` reads a Lance dataset from a path or object-store URI; serve it
under a contract with `bind_table`. Scans stream; projections, limits and filters that cannot
fail are passed to Lance, and DataFusion re-checks the filters. Lance uses an older Arrow than
peQL, so batches cross by Arrow IPC.
