# Contributing

The contributor workflow is in the repository's
[CONTRIBUTING.md](https://github.com/griot-cloud/peql/blob/main/CONTRIBUTING.md).

Before opening a pull request, run:

```bash
cargo fmt --all --check
cargo clippy --all-targets --features flight,signed-bundle,s3 -- -D warnings
cargo test --features flight,signed-bundle,s3
examples/run-all.sh
```

The `lance` feature needs `protoc`. For documentation changes, also build the site:

```bash
python -m pip install -r docs/requirements.txt
sphinx-build -W --keep-going -b html docs docs/_build/html
```
