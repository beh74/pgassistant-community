"""Heuristic thresholds for the query-level index advisor.

Fractions are in [0, 1], durations in milliseconds, and planner costs in
PostgreSQL cost units (not milliseconds). These values preserve the existing
policy; separate names keep independently tunable decisions explicit.
"""

# Indexed access: maximum fraction surviving the residual filter (inclusive).
# Estimates allow more candidates for review than measured execution does.
INDEXED_FILTER_MAX_ACTUAL_KEPT_FRACTION = 0.30
INDEXED_FILTER_MAX_ESTIMATED_KEPT_FRACTION = 0.50
INDEXED_FILTER_MIN_REMOVED_ROWS = 100
INDEXED_SCAN_MIN_TOTAL_TIME_MS = 1.0  # Cumulative across execution loops.

# Sequential access: reject candidates selecting this fraction or more.
SEQ_SCAN_MAX_SELECTED_FRACTION = 0.50
SINGLE_COLUMN_MAX_ESTIMATED_SELECTIVITY = 0.20
SEQ_SCAN_MIN_TOTAL_TIME_MS = 1.0  # Cumulative across execution loops.
FULL_SCAN_MIN_TABLE_FRACTION = 0.50

# ORDER BY / GROUP BY: per-loop node time; spills/workload can also qualify.
SORT_MIN_TIME_MS = 1.0
GROUP_BY_MIN_TIME_MS = 1.0

# Any of these inclusive bounds qualifies a table as small.
SMALL_TABLE_MAX_PAGES = 8
SMALL_TABLE_MAX_ROWS = 1000
SMALL_TABLE_MAX_BYTES = 128 * 1024

# Unexecuted scans: absolute cost, or a page-relative cost with a floor.
PLANNED_SCAN_MIN_COST = 100.0
PLANNED_SCAN_MIN_RELATIVE_COST = 10.0
PLANNED_SCAN_COST_PER_PAGE = 0.25

# Any inclusive bound qualifies a pg_stat_statements workload as significant.
HIGH_WORKLOAD_MIN_CALLS = 1000
HIGH_WORKLOAD_MIN_TOTAL_EXEC_TIME_MS = 5000
HIGH_WORKLOAD_MIN_MEAN_EXEC_TIME_MS = 5

# Statistics: minimum MCV probability mass for estimating range selectivity.
RANGE_SELECTIVITY_MIN_MCV_COVERAGE = 0.80

# Multiplicative row-estimation gaps; diagnostic bands use inclusive maxima.
DEFAULT_ROW_ESTIMATION_GAP_FACTOR = 5.0
INDEXED_ROW_ESTIMATION_GAP_FACTOR = 3.0
ROW_ESTIMATION_CLOSE_MAX_FACTOR = 1.5
ROW_ESTIMATION_MODERATE_MAX_FACTOR = 3.0
ROW_ESTIMATION_LARGE_MAX_FACTOR = 10.0
