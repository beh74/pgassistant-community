# pgAssistant Global Advisor Recommendations

This document summarizes the deterministic checks currently included in the pgAssistant Global Advisor.

The Global Advisor is designed to provide database-wide recommendations based on PostgreSQL catalog views, statistics views, and configuration settings. These checks are deterministic: they do not rely on AI to detect issues.

## Scoring Model

Each recommendation can define the following metadata:

| Field | Meaning |
|---|---|
| `confidence` | How reliable the detection logic is. |
| `impact` | Expected performance, reliability, maintainability, or design impact. |
| `effort` | Estimated implementation cost. |
| `manual_review_required` | Whether the recommendation should be reviewed before applying the suggested SQL. |
| `enabled_by_default` | Whether the check is active by default. |

## Recommendation Categories

| Category | Purpose |
|---|---|
| `DESIGN` | Schema design, relational modeling, data integrity. |
| `INDEX` | Missing, redundant, unused, invalid, or oversized indexes. |
| `STATISTICS` | Planner statistics freshness and table analysis. |
| `MAINTENANCE` | Vacuum, bloat, and maintenance-related findings. |
| `CONFIGURATION` | PostgreSQL settings that affect diagnostics, maintenance, or performance. |

---

# Advisor Checks

## 1. Foreign key columns with different data types

Detects foreign key columns whose data type differs from the referenced column type.

This can cause implicit casts, prevent efficient index usage, and introduce unnecessary planning or execution overhead.

**Suggested action:** align the foreign key column type with the referenced column type.

---

## 2. Missing useful foreign key indexes

Detects foreign keys that do not have a supporting index on the child table.

This can slow down joins and may significantly impact `UPDATE` or `DELETE` operations on the referenced parent table.

**Suggested action:** create a concurrent index on the foreign key columns when the child or referenced table is significant.

---

## 3. Non-unique indexes covered by unique indexes

Detects non-unique indexes that are fully covered by equivalent unique indexes.

When both indexes use the same column set and index properties, the unique index can satisfy the same lookup patterns.

**Suggested action:** drop the redundant non-unique index, preferably with `DROP INDEX CONCURRENTLY`.

---

## 4. Strictly duplicate unused indexes

Detects strictly duplicate non-unique indexes with identical definitions where the duplicate index has not been used.

Duplicate indexes increase storage consumption and write overhead without adding value.

**Suggested action:** keep one index and drop the duplicate unused indexes.

---

## 5. Unused indexes not supporting constraints

Detects indexes with `idx_scan = 0` that do not support primary keys, unique constraints, exclusion constraints, or foreign key access patterns.

The finding is based on PostgreSQL statistics since the last statistics reset.

**Suggested action:** consider dropping the unused index after reviewing workload history and application behavior.

---

## 6. Tables with potentially stale statistics

Detects tables whose planner statistics may be stale because they have never been analyzed, are old, or have significant data churn since the last analyze.

Stale statistics can lead PostgreSQL to choose poor execution plans.

**Suggested action:** run `ANALYZE` on the affected tables.

---

## 7. Tables with estimated bloat

Detects tables with a high estimated amount of dead tuples and possible table bloat.

This check uses PostgreSQL statistics and should be interpreted as an estimate, not as an exact physical bloat measurement.

**Suggested action:** review vacuum activity and consider `VACUUM (ANALYZE)`. For severe bloat, a stronger maintenance operation such as `VACUUM FULL`, `CLUSTER`, or `pg_repack` may be required.

---

## 8. Important PostgreSQL settings disabled or suboptimal

Checks important PostgreSQL settings that affect maintenance, diagnostics, and write behavior.

The current check covers:

- `autovacuum`
- `track_counts`
- `track_activities`
- `log_checkpoints`
- `log_autovacuum_min_duration`
- `checkpoint_completion_target`
- `checkpoint_timeout`

**Suggested action:** adjust the affected setting with `ALTER SYSTEM SET ...` and reload the configuration with `SELECT pg_reload_conf();`.

---

## 9. Invalid or unusable indexes

Detects indexes that are invalid, not ready, or not live according to `pg_index`.

This often indicates an interrupted or failed `CREATE INDEX CONCURRENTLY`, or a transient index state.

**Suggested action:** drop the invalid or unusable index concurrently and recreate it if the workload still requires it.

---

## 10. Tables never vacuumed or autovacuumed

Detects tables that have never been vacuumed or autovacuumed according to `pg_stat_user_tables`.

A `NULL` value can also occur after statistics reset, so the result requires context.

**Suggested action:** run `VACUUM (ANALYZE)` and verify that autovacuum is active and effective.

---

## 11. Tables without primary key

Detects user tables that do not have a primary key.

A missing primary key can affect data integrity, application behavior, replication patterns, and maintenance operations.

**Suggested action:** review the table structure and add a primary key where appropriate.

---

## 12. High index-to-table size ratio

Detects medium or large tables whose total index size is disproportionately high compared to the table size.

Small tables are intentionally ignored to avoid noisy recommendations.

**Suggested action:** review indexes on the table, especially unused, redundant, duplicate, or low-value indexes.

---

## 13. Low or missing foreign key coverage

Detects schemas with no foreign keys despite having multiple user tables, or large schemas with very low foreign-key-to-table coverage.

This is a design-oriented heuristic. Some workloads intentionally avoid foreign keys, so manual review is required.

**Suggested action:** review schema relationships and add foreign keys where data integrity should be enforced.

---

# Notes and Limitations

## PostgreSQL statistics reset

Several checks depend on PostgreSQL cumulative statistics views such as:

- `pg_stat_user_tables`
- `pg_stat_user_indexes`

These statistics are reset when PostgreSQL statistics are reset or after some operational events. Findings such as `idx_scan = 0`, `last_vacuum IS NULL`, or `last_analyze IS NULL` should be interpreted in that context.

## Manual review

Some recommendations intentionally require manual review. This is especially important for:

- unused indexes
- index-to-table size ratio
- low foreign key coverage
- table bloat estimation
- missing primary keys

The Global Advisor should surface high-value signals, but schema and workload context still matter.
