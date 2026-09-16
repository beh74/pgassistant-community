<p align="center">
  <img src="media/pgassistant_logo.png" alt="pgAssistant" height="120px"/>
</p>

<h1 align="center">pgAssistant</h1>

<p align="center">
  <strong>A continuous PostgreSQL improvement platform.</strong><br/>
  Analyze PostgreSQL, turn findings into an actionable remediation plan, track what changed, and measure whether the workload actually improved.<br/>
  Designed for one database or a fleet of thousands.
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

pgAssistant is an open-source platform that turns PostgreSQL evidence into prioritized action. It combines database introspection, workload analysis, specialized advisors, implementation planning, historical evidence, and result measurement in a single web interface.

[pgAssistant Collector](https://github.com/beh74/pgassistant-collector) extends this loop with historical workload and environment measurements, connecting recommendations to observed outcomes. Together, they help teams answer four recurring questions:

1. **What should be improved?** Detect and prioritize PostgreSQL risks, inefficiencies, and tuning opportunities.
2. **What did we decide to do?** Consolidate recommendations into an ordered Executive Plan owned by DEV, OPS, or both.
3. **What did we actually fix?** Track the recommendation lifecycle—new, persistent, and no-longer-detected findings—without falsely assuming that disappearance proves deployment.
4. **What was the result?** Correlate changes with execution time, call volume, query mix, PostgreSQL environment changes, and workload impact.

AI assistance is optional. The core advisors remain deterministic and can be used without an LLM.

## See pgAssistant in action

The Executive Plan transforms technical findings into an ordered remediation plan with priorities, ownership, dependencies, and implementation guidance.

![pgAssistant Executive Plan](media/executive_plan.png)

## The continuous improvement loop

pgAssistant is not a real-time monitoring system. It builds a continuous improvement loop from periodic database and workload collections; monitoring platforms remain the source for what is happening right now.

```text
Observe → Diagnose → Prioritize → Plan → Implement → Collect again → Measure
   ↑                                                                  │
   └────────────────────────── Repeat continuously ────────────────────┘
```

pgAssistant supports the full loop:

- **Observe:** Collector captures repeatable workload, environment, and advisor snapshots.
- **Diagnose:** deterministic advisors explain SQL, schema, configuration, maintenance, and lifecycle issues.
- **Prioritize:** findings are ranked by urgency, confidence, effort, and real workload impact.
- **Plan:** Executive Plan turns related recommendations into ordered, team-owned work packages.
- **Verify:** history identifies new and no-longer-detected recommendations while keeping deployment confirmation explicit.
- **Measure:** Workload Insights compares consecutive collections and exposes performance, query mix, environment, and recommendation changes.

The product components feed the same loop rather than producing disconnected reports:

```text
                 PostgreSQL evidence
                         │
       ┌─────────────────┼─────────────────┐
       │                 │                 │
 Global Advisor     Index Advisor   Parameter & maintenance
       │                 │                 │
       └─────────────────┼─────────────────┘
                         ↓
                  Executive Plan
                         ↓
                    Implementation
                         ↓
                  Workload Insights
                         ↓
                    New evidence
```

![workload Insights](media/workload_insight.png)

![workload Insights - Query evolution](media/workload_insight_2.png)

## From findings to action

The Executive Plan groups related findings into ordered, team-owned work packages. It makes dependencies and responsibilities explicit, so teams can move from diagnosis to execution instead of working through an unstructured list of recommendations.

```text
P1 — DEV
  Rewrite query Q42
  Add the missing FK index on orders(customer_id)

P2 — OPS
  Tune autovacuum for the affected high-churn tables
  Review the workload-related PostgreSQL settings

P3 — DEV/OPS
  Drop unsed index, Review index opportunity on public.orders 
```

## Highlights

- **Multi-database support** — connect to a PostgreSQL instance, list its databases, and select the database to analyze.
- **Global Advisor** — deterministic database-wide checks ranked by priority, confidence, impact, and effort.
- **Executive Plan** — combines Global, Index, Parameter, and Autovacuum recommendations into ordered work packages assigned to DEV, OPS, or DEV/OPS.
- **Workload Insights** — compares Collector measurements over time, highlights recommendation and PostgreSQL environment changes, and ranks queries by workload impact.
- **Continuous verification** — tracks new, persistent, and no-longer-detected recommendations without confusing observed disappearance with confirmed implementation.
- **PDF reporting** — generate a styled Executive Plan report filtered for DEV and/or OPS audiences, including a table of contents, maintenance requirements, sources, and SQL commands.
- **Workload prioritization** — identify the queries consuming the largest share of database workload by combining execution frequency and total execution impact.
- **Index Advisor** — inspect workload plans and identify actionable index opportunities, redundant indexes, and foreign-key coverage issues.
- **Parameter Advisor** — review workload signals and propose PostgreSQL parameter changes.
- **Autovacuum Tuning** — analyze cluster and per-table settings, stale statistics, maintenance activity, and table-specific tuning opportunities.
- **Schema and table analysis** — inspect DDL, relationships, table health, indexes, bloat indicators, and partitioned tables.
- **pgTune** — calculate a PostgreSQL configuration baseline through a guided interface.
- **Optional LLM assistance** — request SQL rewrites, schema-design feedback, naming checks, and contextual explanations.


## One database or a PostgreSQL fleet

pgAssistant supports two complementary operating modes:

| Mode | What it provides |
| --- | --- |
| **One database** | Deep-dive SQL, schema, configuration, and maintenance analysis with a concrete remediation plan for developers and DBAs. |
| **PostgreSQL fleet** | Standardized collection, centralized prioritization, historical comparison, and trend analysis across hundreds or thousands of databases. |

For a large PostgreSQL estate, two companion projects automate and centralize the same analyses:

| Project | Role |
| --- | --- |
| **[pgAssistant Collector](https://github.com/beh74/pgassistant-collector)** | Runs selected pgAssistant jobs across declared databases and stores historical snapshots in a central PostgreSQL repository. |
| **[pgAssistant Grafana](https://github.com/beh74/pgassistant-grafana)** | Displays fleet-wide priorities and trends, including the databases requiring attention, ranked queries, advisor findings, and recommendation evolution. |

Together, the three projects let teams:

1. collect consistent diagnostics across environments, applications, groups, and owners;
2. identify which databases should be corrected first;
3. drill down from the fleet overview to a database, query, or recommendation;
4. assign and plan remediation for DEV and OPS;
5. compare each collection with its predecessor, including workload and PostgreSQL environment changes;
6. track whether priority findings remain active or are no longer detected;
7. measure the workload result and decide what to improve next.

```text
PostgreSQL fleet → Collector → Repository → pgAssistant → Executive Plan
       ↑                           ↓             ↓              ↓
       └──────────── Collect again ← Measure results ← Implement changes
                                   ↓
                          Grafana fleet overview
```

## Live demo

Try the database analysis interface at [https://ov-004f8b.infomaniak.ch/](https://ov-004f8b.infomaniak.ch/).

```text
postgresql://postgres:demo@demo-db:5432/northwind
```

Explore the fleet dashboards in the [Grafana demo](https://ov-004f8b.infomaniak.ch/grafana/). The demo credentials are documented in the [pgAssistant Grafana repository](https://github.com/beh74/pgassistant-grafana).

The demo database is reset daily. AI features are disabled: do not enter personal API keys.

## Advisor coverage and implementation plan

The Global, Index, Parameter, Autovacuum, and Fillfactor advisors produce reproducible recommendations. The Executive Plan consolidates related findings by objective or affected object into ordered work packages instead of a disconnected list of checks and SQL commands.

<details>
<summary>View the complete advisor coverage</summary>

The currently available advisors cover:

- **Global Advisor — data model and schema:** foreign-key columns using different data types; tables without a primary key; low or missing foreign-key coverage; and sequences approaching their maximum value.
- **Global Advisor — indexes:** missing useful indexes on foreign keys; non-unique indexes covered by a unique index; strictly duplicate unused indexes; partially duplicate low-usage indexes; unused non-constraint indexes; invalid or unusable indexes; and tables with a high index-to-table size ratio.
- **Global Advisor — statistics, storage, and maintenance:** potentially stale table statistics; estimated table bloat and high dead-tuple volume; tables never vacuumed or autovacuumed; urgent dead-tuple cleanup; tables flagged for autovacuum maintenance; cluster-wide autovacuum load; tables responsible for significant PostgreSQL buffer-cache misses; and abnormally long-running transactions.
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

### Executive Plan PDF reporting

![Executive Plan PDF reporting](media/executive_plan_report.png)

### Workload prioritization

![Workload prioritization](media/query_ranking.png)

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

## Integration API

pgAssistant provides a versioned API for integrating its analysis and planning
capabilities with automation platforms, database portals, CI/CD workflows, and
other operational tools. These endpoints do not depend on a browser session and
do not change the historical `/api/v1` routes.

| Endpoint | Purpose |
| --- | --- |
| `POST /api/v2/pgtune` | Generate a PostgreSQL configuration baseline from explicit resources or an optional database connection. |
| `POST /api/v2/executive-plan` | Run the advisors and return a prioritized, integration-friendly Executive Plan. |

Authentication is optional and controlled by `PGA_API_TOKEN`. When configured,
clients must send `Authorization: Bearer <token>`; when it is unset or empty,
authentication is disabled.

- [API v2 integration guide](docs/api-v2.md)
- Swagger UI: `/api/v2/docs`
- Machine-readable Swagger specification: `/api/v2/swagger.json`

## PostgreSQL access

pgAssistant accepts standard libpq-compatible PostgreSQL connection URIs, including additional connection options:

```text
postgresql://user:password@host:5432/database
```

For the best workload analysis, enable `pg_stat_statements`. Some features degrade gracefully when the extension is unavailable, and generic-plan advisors require PostgreSQL 16 or newer.

Use a dedicated database account with only the permissions required for the analyses you intend to run. Multi-database mode also requires the account to be able to connect to each selected database.

PostgreSQL release metadata is cached for 30 days in `postgresql_versions_cache.json`. The Docker image stores it in `/home/pgassistant/data/postgresql_versions_cache.json`. Set `PGA_POSTGRESQL_VERSIONS_CACHE_FILE` to use another location or mount `/home/pgassistant/data` to preserve the cache when containers are replaced.

## Continuous improvement with pgAssistant Collector

Set the optional `COLLECTOR_URI` environment variable to connect pgAssistant to
a pgAssistant Collector repository:

```text
COLLECTOR_URI=postgresql://collector_reader:password@collector-host:5432/pga_collector
```

When configured, the Database connection page displays a **Collector** tab where
the user searches for and selects the target associated with the active database.
The selection is stored in the session and shared by Executive Plan history,
Query activity history, and Workload Insights. The Executive Plan History tab
provides 7, 15, 30-day and complete-history comparisons, DEV/OPS team filters,
and work-package-grouped changes. The latest collector snapshot is also compared
with a freshly generated Executive Plan, so a correction appears immediately;
the next complete collection confirms it. Partial live plans never confirm a
correction.

Workload Insights compares consecutive periodic measurements—not a real-time
monitoring stream. It correlates recommendation changes with execution time,
call volume, query type, PostgreSQL version and settings changes, and the queries
with the largest workload impact. This provides evidence about what changed after
a decision while preserving the distinction between correlation, a
no-longer-detected finding, and a confirmed deployment.

The collector connection is opened in read-only mode. A dedicated read-only
PostgreSQL role is recommended.


## Documentation and releases

- [Documentation](https://beh74.github.io/pgassistant-blog/)
- [Full changelog](CHANGELOG.md)
- [Docker Hub](https://hub.docker.com/r/bertrand73/pgassistant)
- [Issue tracker](https://github.com/beh74/pgassistant-community/issues)
- [Collector repository](https://github.com/beh74/pgassistant-collector)
- [Grafana dashboards repository](https://github.com/beh74/pgassistant-grafana)

## Who is it for?

| Role | What pgAssistant helps with |
| --- | --- |
| **Developer** | SQL performance, index opportunities, schema design, and actionable implementation guidance. |
| **DBA** | Reproducible diagnostics, configuration, maintenance, autovacuum, and database health. |
| **SRE / Operations** | Workload evolution, operational risks, environment changes, and fleet priorities. |
| **Platform team** | Standardized PostgreSQL analysis and remediation tracking across hundreds or thousands of databases. |
| **Engineering manager / Tech lead** | Prioritized plans, clear DEV/OPS ownership, remediation progress, and measurable results. |
| **Team without dedicated PostgreSQL expertise** | A shareable implementation plan instead of an unexplained list of findings. |

## License

pgAssistant is released under the [MIT License](https://opensource.org/license/mit).
