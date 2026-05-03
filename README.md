<p align="center">
  <img src="media/pgassistant_logo.png" alt="pgAssistant" height="120px"/>
</p>

<h1 align="center">pgAssistant</h1>

<p align="center">
  <strong>PostgreSQL performance and schema analysis tool</strong><br/>
  Understand and improve PostgreSQL workloads using execution plans and database context.
</p>

<p align="center">
  <a href="https://beh74.github.io/pgassistant-blog/">
    <img src="https://img.shields.io/badge/Documentation-pgAssistant-blue?logo=readthedocs">
  </a>
  <a href="https://opensource.org/license/mit">
    <img src="https://img.shields.io/badge/License-MIT-green.svg">
  </a>
  <a href="https://hub.docker.com/r/bertrand73/pgassistant">
    <img src="https://img.shields.io/docker/pulls/bertrand73/pgassistant?label=Docker%20Pulls">
  </a>
  <img src="https://img.shields.io/docker/image-size/bertrand73/pgassistant/latest">
  <img src="https://img.shields.io/docker/v/bertrand73/pgassistant?sort=semver">
</p>

<p align="center">
⭐ If pgAssistant is useful to you, consider starring the repository.
</p>

---

# What is pgAssistant?

pgAssistant is an open-source tool that helps developers **analyze and improve PostgreSQL databases**.

It focuses on combining **database introspection** with **practical recommendations**, rather than just displaying metrics.

Main capabilities:

- **Global Advisor**: database-wide deterministic analysis  
- Query analysis based on real execution plans (`EXPLAIN ANALYZE`)  
- Schema inspection (DDL) with relational visualization  
- Detection of common structural issues (indexes, foreign keys, data types)  
- Database configuration and statistics checks  
- Optional AI-assisted analysis  

---

## Deterministic analysis

Since version 2.8, pgAssistant introduces the **Global Advisor**.

It runs a set of checks directly against PostgreSQL system catalogs and produces a list of recommendations.

Each recommendation includes:

- A **rank** (priority)
- A **confidence level**
- An estimated **impact**
- An estimated **effort**
- A suggested SQL statement when relevant

Typical findings include:

- Missing or unused indexes  
- Redundant indexes  
- Foreign key issues  
- Outdated statistics  
- Table bloat or missing maintenance  
- Configuration problems  

This analysis is deterministic: given the same database state, it produces the same results.

---

## Query-level analysis

pgAssistant can also analyze individual queries using real execution plans:

- `EXPLAIN ANALYZE` parsing  
- Index suggestions  
- Join and execution strategy insights  

This is useful for investigating specific slow queries.

---

## Optional AI assistance

AI can be enabled as an additional layer, but is not required.

It can help with:

- Query rewrites  
- Additional optimization suggestions  
- Naming and convention checks  

When used, AI is provided with database context (schema, statistics, plans) to improve relevance.

---

## Changelog

[View the full changelog](CHANGELOG.md)

---

## Screenshots

## Dashboard
![Dashboard](media/dashboard.png)


## Global advisor summary
![Global advisor](media/global_advisor_summary.png)

## Global advisor recommandations
![Global advisor](media/global_advisor.png)

## Query Insight
![Query Insight](media/analyze_query_insight.png)

## Query Insight Relational view
![Query Insight](media/analyze_relational.png)

## SQL Advisor
![SQL Advisor](media/analyze_advisor.png)

## AI Query Optimization
![LLM Optimize Query](media/llm_optimize_query.png)

---

## Quick Start

### Docker (recommended)

https://beh74.github.io/pgassistant-blog/doc/startup_docker/

### Python (local setup)

https://beh74.github.io/pgassistant-blog/doc/startup_python/

---

## Live demo

https://ov-004f8b.infomaniak.ch/

Demo connection: postgresql://postgres:demo@demo-db:5432/northwind

⚠️ The public demo does not use AI  
⚠️ Do not provide personal API keys

The demo database is reset daily.

---

## Who is it for?

- Developers working with SQL  
- DevOps engineers troubleshooting performance  
- Teams without dedicated database expertise  

---


## Documentation

https://beh74.github.io/pgassistant-blog/

---

## License

MIT

---

## Acknowledgments

UI based on Volt Bootstrap 5 Dashboard:  
https://github.com/themesberg/volt-bootstrap-5-dashboard