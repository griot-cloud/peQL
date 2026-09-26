# Optional integrations

The standard engine works with local Parquet and needs no platform services. Rust applications can add the integrations below when their deployment needs them.

## Signed contract bundles

Enable the `platform` Cargo feature to fetch bundles from an HTTP contract service. `PlatformBundleSource` requests:

```text
GET /v1/contracts/{contract-name}/bundle
```

Configure the source with an authentication header if required. Supplying an ECDSA P-256 verifying key with `with_verifying_key` enables signature verification; without that key, the source relies on its transport and does not verify the signature.

Calling `source.register(&engine, name).await` fetches the bundle, checks that it names the requested contract, and registers it with the engine. Registration verifies the parcel bundle independently of the optional signature check.

In Griot deployments, this contract service is called **T03**. No T03 service is included in peQL.

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

The optional `lance` feature adds a Lance table provider on Unix. `LanceTableProvider::open_uri` opens a dataset by path or object-store URI. `open` reads through Griot's storage service, **T04**, over its Unix socket. Building this feature requires `protoc`.

## Query workers and result signing

`LongRunningPoolManager` runs queued queries on workers sharing an engine. `PoolConfig` controls worker count, queue depth and shutdown drain time. A full queue or a shutting-down pool returns an error to the submitting application.

An optional `EnvelopeSigner` signs result envelopes. `T05Client` implements signing through Griot's notary service, **T05**, on Unix. A signer failure is returned as a pool error.

`K04DEngine` is the Griot integration wrapper for registered bundles and bound data. It checks a bundle handle's tenant against its configured tenant and applies a maximum result-row count after execution. It does not authenticate callers or check that every supplied caller's tenant matches the configured tenant; that remains the host application's responsibility.

These components are library integrations. They do not provide a standalone HTTP query server.
