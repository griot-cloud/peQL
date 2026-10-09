# Python API reference

The public Python package exports `peql.Engine`, `peql.Caller` and `peql.Refused`. `peql.__version__` is a string. The wrapper uses PyArrow tables and schemas; the native extension converts them to Arrow IPC internally. Requires Python 3.9 or later.

| Reference | What it covers |
| --- | --- |
| [Engine](python-engine.md) | Workspace construction and every method, with parameters and results. |
| [Caller](python-caller.md) | Identity, purpose, tenant and optional policy context. |
| [Results and errors](python-results.md) | Return shapes, verdict, envelope and exception mapping. |

See [Using peQL from Python](python.md) for an end-to-end example.

```{toctree}
:hidden:
:maxdepth: 1

python-engine
python-caller
python-results
```
