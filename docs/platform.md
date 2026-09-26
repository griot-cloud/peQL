# Optional integrations

The standard engine works with local Parquet and needs no platform services. Rust applications can add the integrations below when their deployment needs them.

## Signed contract bundles

Enable the `signed-bundle` Cargo feature to accept bundles signed with ECDSA P-256 by their issuer. peQL only verifies; signing belongs to the issuer. `SignedBundle::register(&engine, &key)` checks the signature and then registers the bundle, which recompiles it to its compilation hash. `signing_payload()` returns the exact bytes the issuer signs.

## Custom functions

Parcel supports WebAssembly functions used by contract expressions. Register a module and its function manifest for an owner before compiling contracts that use it:

```text
peql function register module.wasm --manifest function.yaml --owner acme
peql function list
```

The file names above refer to your compiled module and parcel function manifest. peQL verifies the module through parcel-runtime and stores it for that owner. Contracts record the function versions and hashes they use.

Function authoring, the WebAssembly interface and manifest fields belong to parcel. See its [function guide](https://griot-cloud.github.io/parcel/functions.html).

## Other data sources

Rust applications can bind a DataFusion `TableProvider` to a registered contract with `Engine::bind_table`. This lets the application supply data while retaining the contract query path.

`ObjectStoreParquet` serves bindings from a prefix of an object store (`s3://bucket/prefix/`) the way the default resolver serves local directories: streamed listing tables, the same write path, and manifests beside the data. Install it with `Engine::with_bindings`. Pass the store in: `ObjectStoreParquet::new("s3://bucket/prefix/", store)` takes any `object_store` store, configured by the application; the `s3` feature adds the S3 store.

The optional `lance` feature adds a Lance table provider on Unix. `LanceTableProvider::open_uri` opens a dataset by path or object-store URI. Building this feature requires `protoc`.

## Flight SQL

The `flight` feature adds `peql::flight::FlightSql`, a Flight SQL service over tonic on a listener the application supplies (`serve_unix`, `serve_tcp`, or `serve` for any connection stream). `GetFlightInfo` and `CreatePreparedStatement` run `Engine::check`, so a refusal returns before any scan; `DoGet` runs `Engine::query` and puts `{"envelope", "signature"}` JSON in the first message's app metadata; `DoPut` bulk ingest runs `Engine::write` for the contract's owner.

Each request names its caller in the `x-peql-caller` header (a `Caller` as base64 JSON; `caller_header` builds it). The header is trusted, so only whoever authenticated the caller may reach the listener. `FlightSql::for_caller` fixes one caller instead.

## Result signing

`Engine::with_signer` signs every query's envelope with an `EnvelopeSigner`; the result is `QueryResult::signature`. If signing fails, the query fails. `SocketSigner` reaches a signer over a Unix socket, TCP or any connected stream: one connection per envelope, the envelope as one JSON line out, and `{"jws": ...}` or `{"error": ...}` as one line back.
