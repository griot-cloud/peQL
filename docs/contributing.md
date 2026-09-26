# Contributing

Changes to contract syntax or rule meaning belong in parcel. Workspace storage, data writes, query execution and integrations belong in peQL.

## Build and check

Use Rust 1.94 or newer. From the repository root:

```bash
cargo fmt --all --check
cargo clippy --all-targets --features platform -- -D warnings
cargo test --features platform
cargo build --bin peql
PEQL="$PWD/target/debug/peql" examples/run-all.sh
```

The absolute binary path matters because the example runner changes directories. Its example workspaces are reset before each run. The default build requires neither `protoc` nor platform services; testing `lance` also requires `protoc`.

For enforcement changes, include a test demonstrating that a caller cannot read the protected data, including through a join, subquery or expression that might expose it indirectly.

## Build the Python package

From the repository root, with a Python virtual environment active:

```bash
python -m pip install maturin pyarrow pytest
cd bindings/python
maturin develop
python -m pytest -q
```

This builds the native extension and installs it into that environment.

## Build the documentation

From the repository root, with a Python environment active:

```bash
python -m pip install -r docs/requirements.txt
sphinx-build -W --keep-going -b html docs docs/_build/html
python -m http.server 8765 --bind 127.0.0.1 --directory docs/_build/html
```

Open `http://127.0.0.1:8765` to review the site. Treat warnings as failures, and run changed examples against the engine before submitting them.

The repository's [contributor guide](https://github.com/griot-cloud/peql/blob/main/CONTRIBUTING.md) covers local parcel development and releases. Report problems with a small contract, sample data, the command or API call, and the full error message.
