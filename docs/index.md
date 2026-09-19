<div class="peql-hero-logo" role="img" aria-label="peQL logo"></div>

# Documentation

peQL is a policy enforcing query engine built on Apache DataFusion. Applications
send SQL and caller context to the engine. For each referenced dataset, peQL
reads a contract definition, determines the caller's access and transformations,
and enforces them in the query plan. A contract defines the rules; peQL performs
the checks and executes the transformations.

::::{grid} 1 2 2 2
:gutter: 3

:::{grid-item-card} Run a query
:link: getting-started
:link-type: doc

Build the repository, run a working example, then query a Parquet file using
your own JSON contract.
:::

:::{grid-item-card} Use the APIs
:link: USAGE
:link-type: doc

Query from Rust or Python, retrieve scan statistics, and use graph functions.
:::

:::{grid-item-card} Understand execution
:link: concepts
:link-type: doc

Follow a dataset reference from SQL planning through contract resolution and
the physical operators.
:::

:::{grid-item-card} Contribute
:link: contributing
:link-type: doc

Build the site, run the test suite, and locate the implementation for a change.
:::
::::

## The query path

```text
SQL + caller
    ↓
Resolve dataset → evaluate contract definition → allow or deny
    ↓ allow
Open binding → scan → filter rows → mask columns → optional noise
    ↓
Apply exposed columns and query operators → Arrow results
```

For the high-level Rust API and Python binding, table references pass through
this path. The lower-level `K04DEngine` has a different API and enforcement
behavior; see {doc}`reference` before using it.

```{toctree}
:maxdepth: 1
:caption: Learn
:hidden:

getting-started
```

```{toctree}
:maxdepth: 2
:caption: How-to
:hidden:

USAGE
```

```{toctree}
:maxdepth: 2
:caption: Reference
:hidden:

reference
CONTRACT-FORMAT
GRAPH-QUERY
```

```{toctree}
:maxdepth: 2
:caption: Explanation
:hidden:

concepts
ARCHITECTURE
```

```{toctree}
:maxdepth: 1
:caption: Contribute
:hidden:

contributing
changelog
```
