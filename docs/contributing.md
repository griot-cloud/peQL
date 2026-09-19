# Contributing

The contributor workflow and release hygiene live in the repository's
[CONTRIBUTING.md](https://github.com/griot-cloud/peQL/blob/main/CONTRIBUTING.md).

Before opening a pull request, run:

```bash
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test
```

For documentation changes, also build the site locally:

```bash
python -m pip install -r docs/requirements.txt
sphinx-build -W --keep-going -b html docs docs/_build/html
```
