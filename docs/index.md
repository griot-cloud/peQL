<div class="peql-hero-logo" role="img" aria-label="peQL logo"></div>

# Documentation

peQL is a query engine where every table is a data contract. Contracts are written in
[parcel](https://griot-cloud.github.io/parcel/); peQL stores them, writes data under them, and
answers SQL through them. Each caller gets exactly what the contract allows, enforced inside
the query plan.

## The query path

A partner asks for the average salary. The contract adds noise to each salary before anything
is computed, because the caller is not the owner.

<figure class="query-diagram" aria-label="A salary query and a parcel contract enter peQL. The engine reads payroll data through the contract's view, which adds noise to salaries, then computes and returns the average.">
  <div class="query-inputs">
    <div class="query-box">
      <strong>SQL</strong>
      <pre>SELECT AVG(salary) AS avg_salary
FROM "hr/payroll";</pre>
    </div>
    <div class="query-box">
      <strong>Contract (parcel)</strong>
      <pre>- op: shape
  operator: noise
  column: salary
  params: {sensitivity: 1000,
           epsilon: 1, at: row}
  unless: ctx.tenant == 'hr'</pre>
    </div>
  </div>
  <div class="query-join" aria-hidden="true"></div>
  <div class="query-engine">
    <strong>peQL</strong>
    <span>Runs the contract's view inside the plan</span>
    <div class="query-execution">
      <div class="query-source">
        <strong>Source dataset</strong>
        <span>hr/payroll</span>
        <pre>salary
 60,000
 80,000
100,000</pre>
      </div>
      <div class="query-data-arrow" aria-hidden="true">→</div>
      <div class="query-steps">
        <div class="query-step">Read salaries</div>
        <div class="query-step-arrow" aria-hidden="true">↓</div>
        <div class="query-step query-noise">Add noise<span>The contract's projection, for this caller</span></div>
        <div class="query-step-arrow" aria-hidden="true">↓</div>
        <div class="query-step">Compute average</div>
      </div>
    </div>
  </div>
  <div class="query-arrow" aria-hidden="true">↓</div>
  <div class="query-box query-result">
    <strong>Result</strong>
    <pre>avg_salary
79,842.67</pre>
  </div>
  <figcaption>Illustrative result for a non-owner; noise varies each run, and each run spends privacy budget.</figcaption>
</figure>

::::{grid} 1 2 2 2
:gutter: 3

:::{grid-item-card} Quickstart
:link: getting-started
:link-type: doc

Write data under a contract and query it as three callers.
:::

:::{grid-item-card} Use peQL
:link: USAGE
:link-type: doc

The command line, the Rust API, and Python.
:::

:::{grid-item-card} How it works
:link: concepts
:link-type: doc

Views, the gate, shapes, and the write path.
:::

:::{grid-item-card} parcel and peQL
:link: parcel-and-peql
:link-type: doc

What each project does, and the bundle that passes between them.
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
graphs
platform
migrating
```

```{toctree}
:maxdepth: 2
:caption: Reference
:hidden:

reference
```

```{toctree}
:maxdepth: 2
:caption: Explanation
:hidden:

concepts
ARCHITECTURE
parcel-and-peql
```

```{toctree}
:maxdepth: 1
:caption: Contribute
:hidden:

contributing
changelog
```
