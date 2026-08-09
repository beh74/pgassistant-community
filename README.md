<p align="center">
  <img src="media/pgassistant_logo.png" alt="pgAssistant" height="120px"/>
</p>

<h1 align="center">pgAssistant</h1>

<p align="center">
  <strong>Analyze one PostgreSQL database. Prioritize a fleet of thousands.</strong><br/>
  Turn SQL, schema, workload, and fleet diagnostics into clear explanations, actionable recommendations, and an implementation plan.
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


## Managing hundreds or thousands of databases

For a large PostgreSQL estate, two companion projects automate and centralize the same pgAssistant analyses:

| Project | Role |
| --- | --- |
| **[pgAssistant Collector](https://github.com/beh74/pgassistant-collector)** | Runs selected pgAssistant jobs across declared databases and stores historical snapshots in a central PostgreSQL repository. |
| **[pgAssistant Grafana](https://github.com/beh74/pgassistant-grafana)** | Displays fleet-wide priorities and trends, including the databases requiring attention, ranked queries, advisor findings, and recommendation evolution. |

Together, the three projects let teams:

1. collect consistent diagnostics across environments, applications, groups, and owners;
2. identify which databases should be corrected first;
3. drill down from the fleet overview to a database, query, or recommendation;
4. assign and plan remediation for DEV and OPS;
5. track whether priority findings are resolved over time.

```text
PostgreSQL fleet → Collector → pgAssistant → Repository → Grafana
                                  ↓
                       Diagnosis and action plan
```

pgAssistant complements monitoring and alerting platforms: observability shows what is happening; pgAssistant helps decide what to improve next and how to implement it.

## Live demo

Try the database analysis interface at [https://ov-004f8b.infomaniak.ch/](https://ov-004f8b.infomaniak.ch/).

```text
postgresql://postgres:demo@demo-db:5432/northwind
```

Explore the fleet dashboards in the [Grafana demo](https://ov-004f8b.infomaniak.ch/grafana/). The demo credentials are documented in the [pgAssistant Grafana repository](https://github.com/beh74/pgassistant-grafana).

## Advisor coverage and implementation plan

The Global, Index, Parameter, Autovacuum, and Fillfactor advisors produce reproducible recommendations. The Executive Plan consolidates related findings by objective or affected object into ordered work packages instead of a disconnected list of checks and SQL commands.

<details>
<summary>View the complete advisor coverage</summary>

The currently available advisors cover:

- **Global Advisor — data model and schema:** foreign-key columns using different data types; tables without a primary key; low or missing foreign-key coverage; and sequences approaching their maximum value.
- **Global Advisor — indexes:** missing useful indexes on foreign keys; non-unique indexes covered by a unique index; strictly duplicate unused indexes; partially duplicate low-usage indexes; unused non-constraint indexes; invalid or unusable indexes; and tables with a high index-to-table size ratio.
- **Global Advisor — statistics, storage, and maintenance:** potentially stale table statistics; estimated table bloat and high dead-tuple volume; tables never vacuumed or autovacuumed; urgent dead-tuple cleanup; tables flagged for autovacuum maintenance; cluster-wide autovacuum load; and abnormally long-running transactions.
- **Global Advisor — configuration and lifecycle:** important PostgreSQL settings that are disabled or suboptimal; unsupported PostgreSQL major versions; and available minor-version upgrades.
- **Index Advisor — query plans:** index opportunities for selective sequential scans and residual filters; safer single-column or composite index candidates; indexes supporting joins; indexes supporting `ORDER BY`, including `ORDER BY ... LIMIT`; indexes supporting `GROUP BY`; existing equivalent-index detection; and row-estimation or statistics observations that make an automatic recommendation unsafe.
- **Parameter Advisor — workload configuration:** reviews of `work_mem`, `effective_cache_size`, `random_page_cost`, `effective_io_concurrency`, `max_parallel_workers_per_gather`, and `max_wal_size` based on workload and generic-plan signals.
- **Autovacuum Advisor — per-table actions:** `ANALYZE`, `VACUUM`, and table-specific autovacuum tuning for never-analyzed, stale-analysis, never-vacuumed, stale-vacuum, modified-row, and dead-tuple pressure conditions.
- **Autovacuum Advisor — cluster settings:** reviews of `autovacuum`, `autovacuum_max_workers`, `autovacuum_naptime`, vacuum and analyze scale factors and thresholds, `autovacuum_vacuum_cost_delay`, `autovacuum_vacuum_cost_limit`, and `log_autovacuum_min_duration`.
- **Fillfactor Advisor:** identifies tables that may benefit from a controlled fillfactor experiment, validates the signal against HOT-update efficiency, indexed-column updates, vacuum pressure, and long-running transactions, and highlights partitioned tables that require leaf-by-leaf review.

</details>

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

PostgreSQL release metadata is cached for 30 days in `postgresql_versions_cache.json`. The Docker image stores it in `/home/pgassistant/data/postgresql_versions_cache.json`. Set `PGA_POSTGRESQL_VERSIONS_CACHE_FILE` to use another location or mount `/home/pgassistant/data` to preserve the cache when containers are replaced.


The demo database is reset daily. AI features are disabled: do not enter personal API keys.

## Documentation and releases

- [Documentation](https://beh74.github.io/pgassistant-blog/)
- [Full changelog](CHANGELOG.md)
- [Docker Hub](https://hub.docker.com/r/bertrand73/pgassistant)
- [Issue tracker](https://github.com/beh74/pgassistant-community/issues)
- [Collector repository](https://github.com/beh74/pgassistant-collector)
- [Grafana dashboards repository](https://github.com/beh74/pgassistant-grafana)

## Who is it for?

- platform, SRE, and database teams responsible for hundreds or thousands of PostgreSQL databases;
- DBAs who need to prioritize work across a fleet and produce reproducible diagnostics;
- DevOps and operations teams planning maintenance and configuration changes at scale;
- engineering managers and technical leads who need a clear view of database risk, ownership, and remediation progress;
- developers who need practical feedback on SQL and schema design;
- teams without dedicated PostgreSQL performance expertise that need a shareable implementation plan rather than a raw list of findings.

## License

pgAssistant is released under the [MIT License](https://opensource.org/license/mit).
