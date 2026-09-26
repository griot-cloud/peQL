---
layout: landing
content_max_width: 68rem
---

<div class="peql-home">

<div class="peql-hero-logo" role="img" aria-label="peQL logo"></div>

# peQL

<p class="home-lead">Policy Enforcing Query Engine (peQL) is a SQL query engine that lets you define different data rules and data access policies for users, agents and services, and enforces those rules whenever they query your data.</p>

<section class="home-usecases" aria-labelledby="usecases-title">
  <h2 id="usecases-title" class="home-section-title">Use cases</h2>
  <div class="home-usecase"><h3>Share data with customers and partners</h3><p>Let each customer query their own records from a shared dataset. Mask personal details for partners while allowing authorised staff to see them.</p></div>
  <div class="home-usecase"><h3>Set limits on what agents can query</h3><p>Give an agent access to the rows and columns its task requires. peQL applies those limits to the SQL it submits.</p></div>
  <div class="home-usecase"><h3>Keep failed quality checks out of reports</h3><p>Exclude rows that fail required checks, or refuse queries when a dataset fails a quality or freshness requirement.</p></div>
  <div class="home-usecase"><h3>Share statistics with privacy rules</h3><p>Suppress results for small groups, add noise to aggregates and limit repeated queries with a privacy budget.</p></div>
</section>

<h2 class="home-section-title">Documentation</h2>
<nav class="home-cards" aria-label="Documentation sections">
  <a class="home-card" href="getting-started.html"><span class="card-number">01</span><h3>Getting started</h3><p>Learn the concepts, then run your first queries.</p><span class="card-arrow" aria-hidden="true">↗</span></a>
  <a class="home-card" href="USAGE.html"><span class="card-number">02</span><h3>Using peQL</h3><p>Work with the command line, or use peQL in Python and Rust.</p><span class="card-arrow" aria-hidden="true">↗</span></a>
  <a class="home-card" href="execution.html"><span class="card-number">03</span><h3>How it works</h3><p>Understand validation, caller rules and query execution.</p><span class="card-arrow" aria-hidden="true">↗</span></a>
  <a class="home-card" href="reference.html"><span class="card-number">04</span><h3>Reference</h3><p>Look up commands, result fields and common errors.</p><span class="card-arrow" aria-hidden="true">↗</span></a>
</nav>
<a class="home-next" href="getting-started.html"><span><small>Start here</small>Getting started</span><span aria-hidden="true">→</span></a>
</div>

```{toctree}
:maxdepth: 2
:hidden:

getting-started
USAGE
execution
reference
```
