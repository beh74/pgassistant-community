# pgAssistant Advisors

pgAssistant includes several advisors that look at a PostgreSQL database from different angles.

They do not all work the same way:

- the **Index Advisor** analyzes one query execution plan;
- the **Global Advisor** runs deterministic database-wide checks;
- the **Table Advisor** focuses on a table definition and mainly uses AI-assisted review.

The goal is to provide practical recommendations, while keeping enough context visible for the user to decide what is safe to apply.

---

## Index Advisor

The Index Advisor is the query-level advisor.

It is used from the query analysis screen. The user submits a SQL query, pgAssistant runs an execution plan, and the advisor inspects the resulting plan to suggest useful indexes.

### Input

The Index Advisor works from PostgreSQL execution plans, usually generated with:

- `EXPLAIN ANALYZE` when parameter values are provided;
- `EXPLAIN (GENERIC_PLAN TRUE, ...)` on PostgreSQL 16 and later when the user wants a generic plan without supplying parameter values.

The advisor also uses query context such as:

- scan nodes;
- filter predicates;
- join predicates;
- sort operations;
- `ORDER BY` columns;
- `GROUP BY` columns;
- existing index usage;
- table and column statistics when available.

### What It Detects

The Index Advisor looks for common query-level indexing opportunities:

- expensive sequential scans that could benefit from an index;
- filter predicates on columns that are not efficiently indexed;
- joins where the join key may need an index;
- `ORDER BY` patterns where index order can reduce sorting work;
- simple `GROUP BY` patterns where an index may help grouping;
- cases where an existing index is used but may not fully cover the query shape.

It also tries to avoid noisy recommendations. For example, it should not suggest an index when the plan already uses a suitable access path, when the table is too small to justify the index, or when a sort belongs to a grouping operation rather than to a standalone `ORDER BY` optimization.

### Output

Each recommendation is attached to the analyzed query and includes:

- the target schema and table;
- the candidate columns;
- the recommendation type;
- the reason;
- the confidence level;
- optional `ORDER BY` or `GROUP BY` index shape information;
- suggested SQL when pgAssistant can build one safely.

The Index Advisor is intentionally scoped to the current query. It does not try to decide whether an index is globally useful across the whole workload. The user should still review write overhead, index duplication, table size, and workload frequency before applying a new index.

---

## Global Advisor

The Global Advisor is the database-wide deterministic advisor.

It runs a catalog of checks against PostgreSQL system catalogs, statistics views, and configuration values. Unlike the Table Advisor, it does not rely on AI to detect issues.

### Input

The Global Advisor uses:

- the connected PostgreSQL database;
- built-in SQL checks defined in the advisor catalog;
- PostgreSQL catalog views such as `pg_class`, `pg_namespace`, `pg_index`, and `pg_constraint`;
- PostgreSQL statistics views such as `pg_stat_user_tables` and `pg_stat_user_indexes`;
- selected configuration settings;
- PostgreSQL version information.

### Recommendation Catalog

Global Advisor checks are defined as structured recommendations.

Each recommendation can include:

- a stable recommendation identifier;
- a category, such as `DESIGN`, `INDEX`, `STATISTICS`, `MAINTENANCE`, or `CONFIGURATION`;
- a target team, usually `DEV` or `OPS`;
- an advisor group;
- confidence, impact, and effort values;
- safety metadata;
- manual review requirements;
- explanatory text;
- optional SQL to improve the situation.

This makes the Global Advisor deterministic and repeatable: the same database state should produce the same findings.

### What It Detects

Typical Global Advisor findings include:

- foreign key columns with different data types;
- missing useful indexes on foreign keys;
- redundant or duplicate indexes;
- unused indexes;
- tables without primary keys;
- low or missing foreign key coverage;
- stale statistics;
- table bloat indicators;
- autovacuum or maintenance risks;
- invalid or unusable indexes;
- important PostgreSQL settings that are disabled or suboptimal;
- PostgreSQL version upgrade recommendations.

### Ranking

Each recommendation receives a rank from `0` to `100`.

The rank is computed from:

- confidence;
- expected impact;
- estimated effort.

Higher confidence and impact increase the rank. Higher effort decreases it. The rank is used to sort recommendations and identify priorities, but it is not the same thing as a database health score.

### Dashboard DEV Score

The dashboard exposes a DEV-oriented score through the API.

This score is calculated by the backend, not by the dashboard template.

The scoring model starts at `100` and subtracts penalties by recommendation type. Penalties are weighted by:

- priority: `HIGH`, `MEDIUM`, or `LOW`;
- number of affected objects;
- the relevant database scope, such as table count, index count, foreign key count, or column count.

This prevents a single isolated issue from making a large database look unhealthy. For example, one table without a primary key in a database with one thousand tables should have only a small impact, while the same issue across a large share of tables should have a much larger impact.

The API returns the score and related context, including:

- `database_score`;
- `score_penalty`;
- `score_scope`;
- recommendation groups by type;
- per-type affected counts and score penalties.

### Output

The Global Advisor output is a list of recommendations with:

- priority and rank;
- category and advisor group;
- owning team;
- affected object;
- reason and expected benefit;
- fix strategy;
- generated SQL when available;
- safety and manual review metadata.

The user can review recommendations from the Global Advisor page, from dashboard summaries, or through API endpoints.

---

## Table Advisor

The Table Advisor focuses on one table at a time.

It is designed for table design review rather than workload-wide analysis. It combines PostgreSQL context with AI-assisted reasoning, so it is more exploratory than the deterministic Global Advisor.

### Input

The Table Advisor uses table-level context such as:

- generated DDL for the selected table;
- columns and data types;
- primary keys and foreign keys;
- indexes;
- constraints;
- dependency graph information when available;
- optional user-defined SQL design guidelines.

The table page also exposes database-derived signals such as table size, estimated rows, index statistics, dead tuples, and bloat indicators. These help the user decide which tables deserve closer inspection.

### AI-Assisted Checks

The Table Advisor can send table DDL and context to the configured LLM provider.

It can help with:

- reviewing table structure;
- finding candidate primary keys;
- checking naming and SQL conventions;
- identifying design smells;
- suggesting schema improvements;
- explaining possible normalization or constraint issues.

When custom SQL guidelines are configured, pgAssistant can include them in the prompt so the review matches the team conventions.

### Output

The Table Advisor returns an AI-generated analysis for the selected table.

Depending on the selected action, the output may include:

- design observations;
- candidate primary key suggestions;
- naming or convention feedback;
- possible DDL improvements;
- warnings that require human validation.

Because this advisor mainly uses AI, its output should be treated as guidance rather than deterministic truth. It is especially useful for review, learning, and design discussion, but generated suggestions must be validated before being applied to production schemas.

---

## Choosing the Right Advisor

Use the **Index Advisor** when investigating one query and its execution plan.

Use the **Global Advisor** when reviewing the health, design, indexing, maintenance, and configuration state of a whole database.

Use the **Table Advisor** when reviewing the design of one table, especially when human-readable feedback or team-specific conventions matter.

These advisors are complementary. A query may need a better index, a database may have structural issues, and a table may still deserve a design review even when no deterministic warning is raised.
