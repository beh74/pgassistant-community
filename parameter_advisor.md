# Parameter Advisor algorithm

This document explains the current logic used by pgAssistant's Parameter Advisor.

The goal is not to automatically tune PostgreSQL. The goal is to inspect the current workload captured by `pg_stat_statements`, aggregate useful signals, and suggest which pgTune-style parameters deserve review.

## Scope

The advisor focuses on the same family of settings exposed by pgTune:

- `max_connections`
- `shared_buffers`
- `effective_cache_size`
- `maintenance_work_mem`
- `checkpoint_completion_target`
- `wal_buffers`
- `default_statistics_target`
- `random_page_cost`
- `effective_io_concurrency`
- `work_mem`
- `huge_pages`
- `min_wal_size`
- `max_wal_size`
- `max_worker_processes`
- `max_parallel_workers_per_gather`
- `max_parallel_workers`
- `max_parallel_maintenance_workers`

The first implementation only emits recommendations when the workload provides enough evidence for a useful review. Not every pgTune parameter has an automatic recommendation rule yet.

## Requirements

The advisor requires PostgreSQL 16 or newer because it uses:

```sql
EXPLAIN (GENERIC_PLAN TRUE, VERBOSE TRUE, SETTINGS TRUE, FORMAT JSON)
```

Generic plans allow pgAssistant to inspect normalized queries from `pg_stat_statements` without knowing the original parameter values used by the application.

## Data Sources

The algorithm uses two complementary sources.

### pg_stat_statements

`pg_stat_statements` provides runtime workload counters:

- number of captured queries
- calls
- total execution time
- shared block hits and reads
- temporary blocks read and written
- WAL records, full page images and WAL bytes

Temporary block metrics come from `pg_stat_statements`, not from the generic plan. A generic plan is not executed, so it cannot produce real spill-to-disk counters.

### Generic Plans

For each captured query, pgAssistant asks PostgreSQL for a generic JSON plan and aggregates structural plan signals:

- sort nodes
- hash and hash join nodes
- aggregate nodes
- sequential scan nodes
- index and bitmap scan nodes
- materialize nodes
- gather and gather merge nodes
- parallel-aware nodes
- workers planned
- plan cost and estimated rows

Queries that use PostgreSQL internal schemas such as `pg_catalog`, `information_schema` or `pg_toast` are skipped after the generic plan is produced.

## Processing Steps

1. Detect PostgreSQL major version.
2. Read the current values of pgTune-style parameters.
3. Read all usable rows from `pg_stat_statements`.
4. Aggregate runtime counters from `pg_stat_statements`.
5. For each query, run a generic JSON plan.
6. Skip plans that target PostgreSQL internal schemas.
7. Aggregate plan-node metrics across the workload.
8. Build parameter review recommendations from the combined workload signals.

## Failure Handling

A query is counted as a failure when PostgreSQL cannot produce a generic plan for it.

Common causes:

- ambiguous `$1`, `$2`, etc. parameter typing
- missing temporary table, function, view or schema
- insufficient privileges on a referenced object
- SQL captured by `pg_stat_statements` no longer matches the current database state
- statements that are not meaningful or valid for `EXPLAIN`

Internal PostgreSQL plans are not counted as failures. They are counted separately as skipped internal queries.

## Recommendation Rules

When a rule proposes a concrete value, the recommendation is not emitted if the current value already matches the proposal.

### work_mem

Recommendation is emitted when:

- temporary blocks were read or written, or
- the workload has many memory-sensitive plan nodes: sorts, hashes and aggregates.

Current proposal:

- if the current `work_mem` can be parsed, propose doubling it;
- otherwise propose `64MB`.

Confidence:

- `high` when temp blocks are present;
- `review` when the signal comes only from plan structure.

Reasoning:

Sorts, hashes and aggregates may need memory. If temporary blocks are present, at least part of the workload spilled to disk.

### effective_cache_size

Recommendation is emitted when:

- shared block reads are present, and
- shared cache hit ratio is below 95%.

Current proposal:

- no direct value is proposed.
- the user is asked to review the value with pgTune using realistic RAM and OS cache assumptions.

Reasoning:

`effective_cache_size` is a planner estimate, not an allocated memory area. A poor value can make PostgreSQL underestimate how much data is likely cached.

### random_page_cost

Recommendation is emitted when:

- shared block reads are present, and
- the generic plans include sequential scans.

Current proposal:

- `1.1`

Confidence:

- always `review`.

Reasoning:

On SSD or NVMe storage, the default cost model may be too conservative if `random_page_cost` still reflects HDD-like behavior. This can influence whether PostgreSQL chooses indexes or sequential scans.

Important:

This proposal should only be considered when the storage is actually SSD/NVMe or equivalent. It should not be blindly applied on slow or highly constrained storage.

### effective_io_concurrency

Recommendation is emitted together with `random_page_cost` when:

- shared block reads are present, and
- generic plans include sequential scans.

Current proposal:

- `200`

Confidence:

- always `review`.

Reasoning:

On SSD/NVMe storage, a higher `effective_io_concurrency` can help PostgreSQL model concurrent I/O more realistically, especially for bitmap and prefetch-heavy access patterns.

### max_parallel_workers_per_gather

Recommendation is emitted when:

- the workload contains enough potentially parallel-friendly nodes, and
- generic plans did not choose any `Gather` or `Gather Merge` node.

Potentially parallel-friendly nodes are counted as:

```text
sequential scans + aggregates + sorts
```

The current threshold is:

```text
parallel_candidates >= max(5, total_queries * 0.15)
```

Current proposal:

- `2`

Confidence:

- always `review`.

Reasoning:

If many plans look parallel-friendly but none use parallel execution, the parallel settings may deserve review. This should be reviewed together with:

- `max_parallel_workers`
- `max_worker_processes`
- CPU count
- workload concurrency

### max_wal_size

Recommendation is emitted when:

- WAL generated by the captured workload is greater than 1 GB.

Current proposal:

- if the current `max_wal_size` can be parsed, propose doubling it;
- otherwise propose `4GB`.

Confidence:

- always `review`.

Reasoning:

High WAL volume may indicate that checkpoint/WAL sizing should be reviewed. This recommendation should be confirmed with checkpoint statistics before changing configuration.

## Output

The API returns:

- support status and detected PostgreSQL version
- current pgTune-style parameter values
- aggregated runtime metrics from `pg_stat_statements`
- aggregated generic-plan metrics
- per-query plan status without returning the full SQL workload
- parameter recommendations with:
  - parameter name
  - current value
  - proposed value when available
  - confidence
  - reason
  - evidence
  - optional `ALTER SYSTEM` command

## Safety Model

The advisor does not apply changes automatically.

Generated SQL is intentionally presented as a proposal:

```sql
ALTER SYSTEM SET parameter = 'value';
SELECT pg_reload_conf();
```

The user must review the recommendation before applying it.

## Known Limits

- Generic plans are estimates, not real executions.
- Generic plans do not provide actual runtime timings, actual rows, buffers or temp spills.
- Temp block evidence comes from historical `pg_stat_statements` counters.
- `pg_stat_statements` is cumulative since the last reset, so old workload phases can influence recommendations.
- The algorithm does not know server RAM, CPU count, storage class or concurrency model unless those are inferred elsewhere.
- Some recommendations therefore deliberately say `review` instead of proposing a precise value.
- The first version does not yet emit rules for every pgTune parameter.

## Design Intent

The advisor should remain conservative. It should help the user identify promising tuning areas, not pretend that a single SQL snapshot can fully replace workload testing, pgTune input quality, and operational judgement.
