PGSS_COLUMN_DOCS = {
    # --- Identification ---
    "userid": "OID of the user who executed the statement.",
    "dbid": "OID of the database in which the statement was executed.",
    "toplevel": "True if the statement was executed as a top-level query.",
    "queryid": "Hash identifier of the normalized query.",
    "query": "Text of a representative statement.",

    # --- Planning ---
    "plans": "Number of times the statement was planned.",
    "total_plan_time": "Total time spent planning the statement (ms).",
    "min_plan_time": "Minimum planning time (ms).",
    "max_plan_time": "Maximum planning time (ms).",
    "mean_plan_time": "Mean planning time (ms).",
    "stddev_plan_time": "Standard deviation of planning time (ms).",

    # --- Execution ---
    "calls": "Number of times the statement was executed.",
    "total_exec_time": "Total execution time of the statement (ms).",
    "min_exec_time": "Minimum execution time (ms).",
    "max_exec_time": "Maximum execution time (ms).",
    "mean_exec_time": "Mean execution time (ms).",
    "stddev_exec_time": "Standard deviation of execution time (ms).",
    "rows": "Total number of rows retrieved or affected.",

    # --- Shared buffers ---
    "shared_blks_hit": "Number of shared buffer cache hits.",
    "shared_blks_read": "Number of shared blocks read from disk.",
    "shared_blks_dirtied": "Number of shared blocks dirtied.",
    "shared_blks_written": "Number of shared blocks written.",

    # --- Local buffers ---
    "local_blks_hit": "Number of local buffer cache hits.",
    "local_blks_read": "Number of local blocks read.",
    "local_blks_dirtied": "Number of local blocks dirtied.",
    "local_blks_written": "Number of local blocks written.",

    # --- Temporary buffers ---
    "temp_blks_read": "Number of temporary blocks read.",
    "temp_blks_written": "Number of temporary blocks written.",

    # --- IO timing (legacy + new) ---
    "blk_read_time": "Time spent reading blocks (ms).",
    "blk_write_time": "Time spent writing blocks (ms).",
    "temp_blk_read_time": "Time spent reading temporary blocks (ms).",
    "temp_blk_write_time": "Time spent writing temporary blocks (ms).",

    # PG17+ split IO timing
    "shared_blk_read_time": "Time spent reading shared blocks (ms).",
    "shared_blk_write_time": "Time spent writing shared blocks (ms).",
    "local_blk_read_time": "Time spent reading local blocks (ms).",
    "local_blk_write_time": "Time spent writing local blocks (ms).",

    # --- WAL ---
    "wal_records": "Number of WAL records generated.",
    "wal_fpi": "Number of WAL full page images generated.",
    "wal_bytes": "Total amount of WAL generated (bytes).",
    "wal_buffers_full": "Number of times WAL buffers became full.",

    # --- JIT ---
    "jit_functions": "Number of functions JIT-compiled.",
    "jit_generation_time": "Time spent generating JIT code (ms).",
    "jit_inlining_count": "Number of JIT inlining operations.",
    "jit_inlining_time": "Time spent inlining JIT code (ms).",
    "jit_optimization_count": "Number of JIT optimization operations.",
    "jit_optimization_time": "Time spent optimizing JIT code (ms).",
    "jit_emission_count": "Number of JIT code emissions.",
    "jit_emission_time": "Time spent emitting JIT code (ms).",

    # PG17+
    "jit_deform_count": "Number of tuple deform JIT operations.",
    "jit_deform_time": "Time spent on tuple deform JIT (ms).",

    # --- Statistics metadata ---
    "stats_since": "Timestamp when statistics collection started.",
    "minmax_stats_since": "Timestamp when min/max statistics started.",

    # --- Parallelism (PG18+) ---
    "parallel_workers_to_launch": "Number of planned parallel workers.",
    "parallel_workers_launched": "Number of launched parallel workers.",

    # --- PgAssistant fields ---
    "hit_cache_ratio": "Ratio of buffer hits to total buffer accesses.",
    "tables": "List of tables involved in the statement - Semantic analysis of the query",
    "operation_type": "Type of operation (e.g., SELECT, INSERT) - Semantic analysis of the query",
    "total_blks_read": "Total number of blocks read (shared + temp + local)",
}


def get_column_description(column_name: str) -> str:
    return PGSS_COLUMN_DOCS.get(column_name, "No description available.")

