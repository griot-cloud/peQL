<div class="peql-hero-logo" role="img" aria-label="peQL logo"></div>

# Documentation

peQL is a policy-enforcing query engine built on Apache DataFusion.
Let people query your data while keeping control over what they can see.

## The query path

<figure class="query-diagram" aria-label="A salary query and a noise policy enter peQL. The engine reads payroll data, adds noise to salaries, then calculates and returns the average.">
  <div class="query-inputs">
    <div class="query-box">
      <strong>SQL</strong>
      <pre>SELECT AVG(salary) AS avg_salary
FROM "hr/payroll";</pre>
    </div>
    <div class="query-box">
      <strong>Policy</strong>
      <pre>"dp_columns": {
  "salary": {
    "sensitivity": 1000,
    "epsilon": 1
  }
}</pre>
    </div>
  </div>
  <div class="query-join" aria-hidden="true"></div>
  <div class="query-engine">
    <strong>peQL</strong>
    <span>Compiles policy into query execution</span>
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
        <div class="query-step query-noise">Add DP noise<span>Apply the policy to each salary</span></div>
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
  <figcaption>The query asks for an average. The policy adds noise before it is calculated.<br>Illustrative result for a non-owner query; noise varies each run.</figcaption>
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
