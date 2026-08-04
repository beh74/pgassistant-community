<p align="center">
  <img src="media/pgassistant_logo.png" alt="pgAssistant" height="120px"/>
</p>

<h1 align="center">pgAssistant</h1>

<p align="center">
  <strong>PostgreSQL insights, advisors, and implementation planning</strong><br/>
  Turn database and workload diagnostics into prioritized, actionable improvements.
</p>

<p align="center">
  <a href="https://beh74.github.io/pgassistant-blog/">
    <img src="https://img.shields.io/badge/Documentation-pgAssistant-blue?logo=readthedocs" alt="Documentation">
  </a>
  <a href="https://opensource.org/license/mit">
    <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT License">
  </a>
  <a href="https://hub.docker.com/r/bertrand73/pgassistant">
    <img src="https://img.shields.io/docker/pulls/bertrand73/pgassistant?label=Docker%20Pulls" alt="Docker pulls">
  </a>
  <img src="https://img.shields.io/docker/image-size/bertrand73/pgassistant/latest" alt="Docker image size">
  <img src="https://img.shields.io/docker/v/bertrand73/pgassistant?sort=semver" alt="Docker image version">
</p>

<p align="center">
  <a href="https://github.com/beh74/pgassistant-community">⭐ Star pgAssistant on GitHub</a> if it helps you improve your PostgreSQL databases.
</p>

---

## What is pgAssistant?

pgAssistant is an open-source PostgreSQL analysis tool for developers, DBAs, and operations teams. It combines database introspection, workload analysis, specialized advisors, and implementation planning in a single web interface.

It does more than display metrics: pgAssistant explains detected issues, proposes concrete actions, identifies the responsible team, and consolidates related recommendations into an ordered plan.

AI assistance is optional. The core advisors remain deterministic and can be used without an LLM .

## Highlights

- **Multi-database support** — connect to a PostgreSQL instance, list its databases, and select the database to analyze.
- **Global Advisor** — deterministic database-wide checks ranked by priority, confidence, impact, and effort.
- **Executive Plan** — combines Global, Index, Parameter, and Autovacuum recommendations into ordered work packages assigned to DEV, OPS, or DEV/OPS.
- **PDF reporting** — generate a styled Executive Plan report filtered for DEV and/or OPS audiences, including a table of contents, maintenance requirements, sources, and SQL commands.
- **Query ranking** — prioritize the workload using execution frequency and total database impact.
- **Index Advisor** — inspect workload plans and identify actionable index opportunities, redundant indexes, and foreign-key coverage issues.
- **Parameter Advisor** — review workload signals and propose PostgreSQL parameter changes.
- **Autovacuum Tuning** — analyze cluster and per-table settings, stale statistics, maintenance activity, and table-specific tuning opportunities.
- **Schema and table analysis** — inspect DDL, relationships, table health, indexes, bloat indicators, and partitioned tables.
- **pgTune** — calculate a PostgreSQL configuration baseline through a guided interface.
- **Optional LLM assistance** — request SQL rewrites, schema-design feedback, naming checks, and contextual explanations.

## Live demo

Try pgAssistant at [https://ov-004f8b.infomaniak.ch/](https://ov-004f8b.infomaniak.ch/).

```text
postgresql://postgres:demo@demo-db:5432/northwind
```

## From findings to an implementation plan

The Global Advisor runs checks against PostgreSQL system catalogs and returns reproducible recommendations. Depending on the finding, a recommendation can include:

- priority and confidence;
- expected impact and implementation effort;
- DEV, OPS, or shared ownership;
- maintenance-window requirements;
- affected database objects;
- executable SQL when applicable.

The Executive Plan then consolidates the results of the available advisors. Related findings are grouped by objective or affected table so that, for example, several schema-design findings become one coherent work package instead of a disconnected list of SQL queries.

Typical findings include:

- missing, unused, duplicate, or partially redundant indexes;
- missing useful indexes on foreign keys;
- inefficient query plans and high-impact queries;
- stale statistics and autovacuum maintenance issues;
- table health and schema-design problems;
- suboptimal PostgreSQL settings;
- long-running or idle transactions;
- unsupported or outdated PostgreSQL versions.

## Query and workload analysis

pgAssistant can analyze an individual SQL statement or the workload collected by `pg_stat_statements`:

- real plans with `EXPLAIN ANALYZE`;
- PostgreSQL 16+ generic plans for parameterized queries;
- joins, scans, sorts, aggregates, buffers, WAL, and row-estimation insights;
- index suggestions supported by schema and column statistics;
- query-parameter mapping and configuration review;
- relational visualization of the tables involved.

> [!CAUTION]
> `EXPLAIN ANALYZE` executes the statement. Review queries carefully and use a suitable database role, especially outside a development environment.

## Optional AI assistance

LLM features are an additional analysis layer and are not required to use pgAssistant. OpenAI-compatible APIs and local providers such as Ollama can be configured from the application settings or container configuration.

When enabled, the LLM receives relevant context such as schemas, statistics, and execution plans to help with:

- SQL rewrites and optimization suggestions;
- schema and relationship reviews;
- SQL naming and convention checks;
- table RFC and standards analysis.

Never expose personal API keys through a shared or public pgAssistant instance.

## Screenshots

### Dashboard

![pgAssistant dashboard](media/dashboard.png)

### Global Advisor summary

![Global Advisor summary](media/global_advisor_summary.png)

### Global Advisor recommendations

![Global Advisor recommendations](media/global_advisor.png)

### Executive Plan

![Executive Plan](media/executive_plan.png)

### Executive Plan PDF reporting

![Executive Plan PDF reporting](media/executive_plan_report.png)

### Query ranking

![Query ranking](media/query_ranking.png)

### Index Advisor

![Index Advisor](media/index_advisor.png)

### Autovacuum tuning

![Autovacuum tuning](media/autovacuum_tuning.png)

## Quick start

### Docker (recommended)

Use the published Docker image and expose the application on port `8080`:

```bash
docker run --name pgassistant --rm -p 8080:5005 bertrand73/pgassistant:latest
```

Then open [http://localhost:8080](http://localhost:8080).

For persistent settings, LLM configuration, Docker Compose, and database connectivity examples, see the [Docker installation guide](https://beh74.github.io/pgassistant-blog/doc/startup_docker/).

### Python

For a local source installation, see the [Python installation guide](https://beh74.github.io/pgassistant-blog/doc/startup_python/).

## PostgreSQL access

pgAssistant accepts standard libpq-compatible PostgreSQL connection URIs, including additional connection options:

```text
postgresql://user:password@host:5432/database
```

For the best workload analysis, enable `pg_stat_statements`. Some features degrade gracefully when the extension is unavailable, and generic-plan advisors require PostgreSQL 16 or newer.

Use a dedicated database account with only the permissions required for the analyses you intend to run. Multi-database mode also requires the account to be able to connect to each selected database.


The demo database is reset daily. AI features are disabled: do not enter personal API keys.

## Documentation and releases

- [Documentation](https://beh74.github.io/pgassistant-blog/)
- [Full changelog](CHANGELOG.md)
- [Docker Hub](https://hub.docker.com/r/bertrand73/pgassistant)
- [Issue tracker](https://github.com/beh74/pgassistant-community/issues)

## Who is it for?

- developers who need practical feedback on SQL and schema design;
- DBAs who want consolidated, reproducible diagnostics;
- DevOps and operations teams planning maintenance and configuration changes;
- teams without dedicated PostgreSQL performance expertise;
- technical leads who need a shareable implementation plan rather than a raw list of findings.

## License

pgAssistant is released under the [MIT License](https://opensource.org/license/mit).
