# Contributing to peQL

peQL is the runtime for [parcel](https://github.com/griot-cloud/parcel) contracts. A change to
what a rule means belongs in parcel; a change to how contracts are stored, written, planned or
served belongs here. See [parcel and peQL](docs/parcel-and-peql.md).

## Setup

Rust 1.94 or newer. The `lance` feature also needs `protoc`.

```bash
git clone https://github.com/griot-cloud/peql && cd peql
cargo test
```

To work on parcel and peQL together, point peQL at a local parcel checkout without committing
it, in `.cargo/config.toml`:

```toml
[patch."https://github.com/griot-cloud/parcel"]
parcel-core = { path = "../parcel/crates/parcel-core" }
parcel-runtime = { path = "../parcel/crates/parcel-runtime" }
```

## Before you open a pull request

```bash
cargo fmt --all --check
cargo clippy --all-targets --features platform -- -D warnings
cargo test --features platform
cargo build --bin peql && PEQL=target/debug/peql examples/run-all.sh
```

- The default build needs no `protoc` and no platform service.
- Enforcement changes need a test that shows a caller cannot see what the contract hides,
  including through a query written to probe for it.
- User-visible behaviour belongs in `docs/` and, if it changes a workflow, in `examples/`.

## Reporting issues

Include the command, the full output, `rustc --version`, and the contract. For an enforcement
bug, a test that builds an `Engine::in_memory` and shows the leak is ideal.
