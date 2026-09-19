<div class="peql-hero-logo" role="img" aria-label="peQL logo"></div>

# Documentation

peQL is a policy-enforcing query engine built on Apache DataFusion.
Let people query your data while keeping control over what they can see.

## The query path

<figure class="query-diagram" aria-label="SQL and policy enter peQL. The engine compiles policy into query execution and returns permitted results.">
  <div class="query-inputs">
    <div class="query-box">
      <strong>SQL</strong>
      <pre>SELECT order_id, total
FROM "sales/orders/v1";</pre>
    </div>
    <div class="query-box">
      <strong>Policy <small>Rego excerpt</small></strong>
      <pre>default allow := false
allow if {
  input.declared_purpose == "analytics"
}</pre>
    </div>
  </div>
  <div class="query-join" aria-hidden="true"></div>
  <div class="query-engine"><strong>peQL</strong><span>Compiles policy into query execution</span></div>
  <div class="query-arrow" aria-hidden="true">↓</div>
  <div class="query-outputs">
    <div class="query-box">
      <strong>Analytics → results</strong>
      <pre>order_id   total
101        49.00
102        85.00</pre>
    </div>
    <div class="query-box query-denied">
      <strong>Other purposes → denied</strong>
      <p>No results returned.</p>
    </div>
  </div>
  <figcaption>Same SQL. The policy determines whether it can run.</figcaption>
</figure>

::::{grid} 1 2 2 2
:gutter: 3

:::{grid-item-card} Quickstart
:link: getting-started
:link-type: doc

Run your first query with a data contract.
:::

:::{grid-item-card} Use the APIs
:link: USAGE
:link-type: doc

Add peQL to a Rust or Python application.
:::

:::{grid-item-card} How it works
:link: concepts
:link-type: doc

See how contracts become enforced query plans.
:::

:::{grid-item-card} Contribute
:link: contributing
:link-type: doc

Build the engine, run tests, and make a change.
:::
::::

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
