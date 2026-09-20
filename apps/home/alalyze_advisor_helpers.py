"""Shared parsing, metadata and heuristic helpers for the index advisor."""

import json
import re
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

from . import index_advisor_thresholds as thresholds
from . import database


# --------------------------------------------------------------------
# Data classes
# --------------------------------------------------------------------

@dataclass
class TableMeta:
    """Database metadata needed to judge table size and existing indexes."""
    schema: str
    table: str
    reltuples: float
    relpages: int
    table_bytes: int
    indexes: List[Dict[str, Any]]


@dataclass
class ScanFinding:
    """Normalized scan node extracted from a PostgreSQL JSON plan."""
    schema: str
    table: str
    alias: Optional[str]
    node_type: str
    index_name: Optional[str]
    index_cond: Optional[str]
    recheck_cond: Optional[str]
    filter_expr: Optional[str]
    actual_rows: float
    plan_rows: float
    actual_loops: float
    startup_cost: float
    total_cost: float
    rows_removed_by_filter: float
    shared_hit_blocks: int
    shared_read_blocks: int
    actual_total_time: float
    parent_node_type: Optional[str] = None
    parent_relationship: Optional[str] = None
    index_def: Optional[str] = None
    has_actual_metrics: bool = True
    estimated_rows_before_filter: Optional[float] = None


@dataclass
class JoinFinding:
    """Normalized join condition extracted from a PostgreSQL JSON plan."""
    join_node_type: str
    join_type: str
    cond_type: str
    cond_expr: str
    left_alias: Optional[str]
    left_column: Optional[str]
    right_alias: Optional[str]
    right_column: Optional[str]
    actual_rows: float
    plan_rows: float
    actual_loops: float
    actual_total_time: float
    shared_hit_blocks: int
    shared_read_blocks: int


@dataclass
class OrderByFinding:
    """Simple Sort/Incremental Sort pattern suitable for ORDER BY analysis."""
    node_type: str
    sort_key: Optional[List[str]]
    presorted_key: Optional[List[str]]
    has_limit: bool
    schema: str
    table: str
    alias: Optional[str]
    child_node_type: str
    child_index_name: Optional[str]
    child_index_cond: Optional[str]
    child_recheck_cond: Optional[str]
    child_filter_expr: Optional[str]
    child_actual_rows: float
    child_plan_rows: float
    actual_rows: float
    plan_rows: float
    actual_loops: float
    startup_cost: float
    total_cost: float
    actual_total_time: float
    shared_hit_blocks: int
    shared_read_blocks: int
    sort_method: Optional[str] = None
    sort_space_type: Optional[str] = None
    sort_space_used: Optional[float] = None


@dataclass
class GroupByFinding:
    """Simple aggregate/grouping pattern suitable for GROUP BY analysis."""
    node_type: str
    strategy: Optional[str]
    group_key: Optional[List[str]]
    schema: str
    table: str
    alias: Optional[str]
    child_node_type: str
    child_index_name: Optional[str]
    child_index_cond: Optional[str]
    child_recheck_cond: Optional[str]
    child_filter_expr: Optional[str]
    child_actual_rows: float
    child_plan_rows: float
    actual_rows: float
    plan_rows: float
    actual_loops: float
    startup_cost: float
    total_cost: float
    actual_total_time: float
    shared_hit_blocks: int
    shared_read_blocks: int
    sort_method: Optional[str] = None
    sort_space_type: Optional[str] = None
    sort_space_used: Optional[float] = None


@dataclass
class QueryStats:
    """pg_stat_statements counters attached to a query when available."""
    queryid: str
    calls: float
    rows: float
    total_exec_time: float
    mean_exec_time: float
    min_exec_time: float
    max_exec_time: float
    stddev_exec_time: float
    shared_blks_hit: float
    shared_blks_read: float
    wal_records: float
    wal_fpi: float
    wal_bytes: float


@dataclass
class ColumnStats:
    """Subset of pg_stats used to estimate selectivity and index usefulness."""
    schema: str
    table: str
    column: str
    null_frac: float
    n_distinct: float
    most_common_vals: Optional[List[Any]]
    most_common_freqs: Optional[List[float]]
    histogram_bounds: Optional[List[Any]]


@dataclass
class Recommendation:
    """Index advisor output returned to API/UI consumers."""
    schema: str
    table: str
    confidence: str
    reason: str
    filter_expr: Optional[str] = None
    candidate_columns: Optional[List[str]] = None
    candidate_order_columns: Optional[List[Dict[str, str]]] = None
    candidate_group_columns: Optional[List[str]] = None
    recommendation_type: Optional[str] = None
    create_index_sql: Optional[str] = None
    existing_index_match: Optional[str] = None
    stats_reason: Optional[str] = None
    node_type: Optional[str] = None
    access_path: Optional[str] = None
    used_index_name: Optional[str] = None
    used_index_def: Optional[str] = None
    index_cond: Optional[str] = None
    recheck_cond: Optional[str] = None
    row_estimation_reason: Optional[str] = None


# --------------------------------------------------------------------
# Session / connection helpers
# --------------------------------------------------------------------

def get_db_config_from_session(session: Dict[str, Any]) -> Dict[str, Any]:
    """Accept the different session shapes used by routes and API calls."""
    if "db_config" in session:
        return session["db_config"]
    if "database" in session:
        return session["database"]
    return session


# --------------------------------------------------------------------
# JSON plan parsing
# --------------------------------------------------------------------

def normalize_plan_json(plan_json: Any) -> Any:
    """Accept either a JSON string or an already decoded plan object."""
    if isinstance(plan_json, str):
        return json.loads(plan_json)
    return plan_json


def extract_root_plan(plan_json: Any) -> Dict[str, Any]:
    """Return the PostgreSQL root Plan node from FORMAT JSON output."""
    if isinstance(plan_json, list):
        if not plan_json:
            raise ValueError("Empty plan JSON.")
        first = plan_json[0]
        if "Plan" not in first:
            raise ValueError("Invalid plan JSON: missing top-level 'Plan'.")
        return first["Plan"]

    if isinstance(plan_json, dict) and "Plan" in plan_json:
        return plan_json["Plan"]

    raise ValueError("Unsupported plan JSON structure.")


def collect_relation_aliases(
    node: Dict[str, Any],
    alias_map: Dict[str, Dict[str, str]],
) -> None:
    """Collect plan aliases so join findings can be mapped back to tables."""
    relation = node.get("Relation Name")
    schema = node.get("Schema")
    alias = node.get("Alias")

    if relation and schema:
        key = alias or relation
        alias_map[key] = {
            "schema": schema,
            "table": relation,
            "alias": alias or relation,
        }

    for child in node.get("Plans", []) or []:
        collect_relation_aliases(child, alias_map)


def walk_plan_collect_findings(
    node: Dict[str, Any],
    scan_findings: List[ScanFinding],
    join_findings: List[JoinFinding],
    order_by_findings: Optional[List[OrderByFinding]] = None,
    group_by_findings: Optional[List[GroupByFinding]] = None,
    parent_node_type: Optional[str] = None,
) -> None:
    """Walk the plan tree and collect scan, join, ORDER BY and GROUP BY findings."""
    node_type = node.get("Node Type", "")

    # ------------------------------------------------------------
    # Scan findings
    # ------------------------------------------------------------
    if node_type in {"Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan"} \
       and node.get("Relation Name") and node.get("Schema"):
        # Only a direct, single bitmap index child provides an unambiguous
        # index identity and pre-filter estimate. Do not sum BitmapAnd/Or rows
        # or compare parallel per-worker output with a global bitmap estimate.
        bitmap_child = None
        children = node.get("Plans", []) or []
        if node_type == "Bitmap Heap Scan" and len(children) == 1:
            if children[0].get("Node Type") == "Bitmap Index Scan":
                bitmap_child = children[0]
        scan_findings.append(
            ScanFinding(
                schema=node["Schema"],
                table=node["Relation Name"],
                alias=node.get("Alias"),
                node_type=node_type,
                index_name=node.get("Index Name") or (bitmap_child or {}).get("Index Name"),
                index_cond=node.get("Index Cond") or (bitmap_child or {}).get("Index Cond"),
                recheck_cond=node.get("Recheck Cond"),
                filter_expr=node.get("Filter"),
                actual_rows=float(node.get("Actual Rows", 0) or 0),
                plan_rows=float(node.get("Plan Rows", 0) or 0),
                actual_loops=float(node.get("Actual Loops", 0) or 0),
                startup_cost=float(node.get("Startup Cost", 0) or 0),
                total_cost=float(node.get("Total Cost", 0) or 0),
                rows_removed_by_filter=float(node.get("Rows Removed by Filter", 0) or 0),
                shared_hit_blocks=int(node.get("Shared Hit Blocks", 0) or 0),
                shared_read_blocks=int(node.get("Shared Read Blocks", 0) or 0),
                actual_total_time=float(node.get("Actual Total Time", 0) or 0),
                parent_node_type=parent_node_type,
                parent_relationship=node.get("Parent Relationship"),
                has_actual_metrics="Actual Loops" in node,
                estimated_rows_before_filter=(
                    float(bitmap_child["Plan Rows"])
                    if bitmap_child is not None and "Plan Rows" in bitmap_child
                    and not node.get("Parallel Aware") else None
                ),
            )
        )

    # ------------------------------------------------------------
    # Join findings
    # ------------------------------------------------------------
    join_cond_type = None
    join_cond_expr = None

    if node_type == "Hash Join" and node.get("Hash Cond"):
        join_cond_type = "Hash Cond"
        join_cond_expr = node.get("Hash Cond")
    elif node_type == "Merge Join" and node.get("Merge Cond"):
        join_cond_type = "Merge Cond"
        join_cond_expr = node.get("Merge Cond")
    elif node_type == "Nested Loop" and node.get("Join Filter"):
        join_cond_type = "Join Filter"
        join_cond_expr = node.get("Join Filter")

    if join_cond_type and join_cond_expr:
        left_alias, left_column, right_alias, right_column = extract_simple_join_columns(join_cond_expr)

        join_findings.append(
            JoinFinding(
                join_node_type=node_type,
                join_type=node.get("Join Type", "Unknown"),
                cond_type=join_cond_type,
                cond_expr=join_cond_expr,
                left_alias=left_alias,
                left_column=left_column,
                right_alias=right_alias,
                right_column=right_column,
                actual_rows=float(node.get("Actual Rows", 0) or 0),
                plan_rows=float(node.get("Plan Rows", 0) or 0),
                actual_loops=float(node.get("Actual Loops", 0) or 0),
                actual_total_time=float(node.get("Actual Total Time", 0) or 0),
                shared_hit_blocks=int(node.get("Shared Hit Blocks", 0) or 0),
                shared_read_blocks=int(node.get("Shared Read Blocks", 0) or 0),
            )
        )

    # ------------------------------------------------------------
    # ORDER BY / Sort findings
    # ------------------------------------------------------------
    if (
        order_by_findings is not None
        and node_type in {"Sort", "Incremental Sort"}
        and parent_node_type not in {"Aggregate", "Group"}
    ):
        child = first_direct_scan_child(node)
        if child is not None:
            order_by_findings.append(
                OrderByFinding(
                    node_type=node_type,
                    sort_key=node.get("Sort Key"),
                    presorted_key=node.get("Presorted Key"),
                    has_limit=parent_node_type == "Limit",
                    schema=child["Schema"],
                    table=child["Relation Name"],
                    alias=child.get("Alias"),
                    child_node_type=child.get("Node Type", ""),
                    child_index_name=child.get("Index Name"),
                    child_index_cond=child.get("Index Cond"),
                    child_recheck_cond=child.get("Recheck Cond"),
                    child_filter_expr=child.get("Filter"),
                    child_actual_rows=float(child.get("Actual Rows", 0) or 0),
                    child_plan_rows=float(child.get("Plan Rows", 0) or 0),
                    actual_rows=float(node.get("Actual Rows", 0) or 0),
                    plan_rows=float(node.get("Plan Rows", 0) or 0),
                    actual_loops=float(node.get("Actual Loops", 0) or 0),
                    startup_cost=float(node.get("Startup Cost", 0) or 0),
                    total_cost=float(node.get("Total Cost", 0) or 0),
                    actual_total_time=float(node.get("Actual Total Time", 0) or 0),
                    shared_hit_blocks=int(node.get("Shared Hit Blocks", 0) or 0),
                    shared_read_blocks=int(node.get("Shared Read Blocks", 0) or 0),
                    sort_method=node.get("Sort Method"),
                    sort_space_type=node.get("Sort Space Type"),
                    sort_space_used=(
                        float(node.get("Sort Space Used"))
                        if node.get("Sort Space Used") is not None
                        else None
                    ),
                )
            )

    # ------------------------------------------------------------
    # GROUP BY / Aggregate findings
    # ------------------------------------------------------------
    if group_by_findings is not None and node.get("Group Key"):
        child = first_simple_group_child(node)
        if child is not None:
            sort_node = child.get("_pga_sort_node")
            scan_node = child.get("_pga_scan_node", child)

            group_by_findings.append(
                GroupByFinding(
                    node_type=node_type,
                    strategy=node.get("Strategy"),
                    group_key=node.get("Group Key"),
                    schema=scan_node["Schema"],
                    table=scan_node["Relation Name"],
                    alias=scan_node.get("Alias"),
                    child_node_type=scan_node.get("Node Type", ""),
                    child_index_name=scan_node.get("Index Name"),
                    child_index_cond=scan_node.get("Index Cond"),
                    child_recheck_cond=scan_node.get("Recheck Cond"),
                    child_filter_expr=scan_node.get("Filter"),
                    child_actual_rows=float(scan_node.get("Actual Rows", 0) or 0),
                    child_plan_rows=float(scan_node.get("Plan Rows", 0) or 0),
                    actual_rows=float(node.get("Actual Rows", 0) or 0),
                    plan_rows=float(node.get("Plan Rows", 0) or 0),
                    actual_loops=float(node.get("Actual Loops", 0) or 0),
                    startup_cost=float(node.get("Startup Cost", 0) or 0),
                    total_cost=float(node.get("Total Cost", 0) or 0),
                    actual_total_time=float(node.get("Actual Total Time", 0) or 0),
                    shared_hit_blocks=int(node.get("Shared Hit Blocks", 0) or 0),
                    shared_read_blocks=int(node.get("Shared Read Blocks", 0) or 0),
                    sort_method=sort_node.get("Sort Method") if sort_node else None,
                    sort_space_type=sort_node.get("Sort Space Type") if sort_node else None,
                    sort_space_used=(
                        float(sort_node.get("Sort Space Used"))
                        if sort_node and sort_node.get("Sort Space Used") is not None
                        else None
                    ),
                )
            )

    for child in node.get("Plans", []) or []:
        walk_plan_collect_findings(
            child,
            scan_findings,
            join_findings,
            order_by_findings=order_by_findings,
            group_by_findings=group_by_findings,
            parent_node_type=node_type,
        )


def first_direct_scan_child(node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Returns the direct child scan for simple Sort -> Scan patterns.
    This deliberately avoids joins and complex subtrees to keep ORDER BY index
    recommendations conservative.
    """
    plans = node.get("Plans", []) or []
    if len(plans) != 1:
        return None

    child = plans[0]
    if child.get("Node Type") in {"Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan"} \
       and child.get("Relation Name") and child.get("Schema"):
        return child

    return None


def first_simple_group_child(node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the simple scan child under a grouping node, allowing one Sort."""
    plans = node.get("Plans", []) or []
    if len(plans) != 1:
        return None

    child = plans[0]
    scan = first_direct_scan_child({"Plans": [child]})
    if scan is not None:
        return scan

    if child.get("Node Type") in {"Sort", "Incremental Sort"}:
        scan = first_direct_scan_child(child)
        if scan is not None:
            return {
                "_pga_sort_node": child,
                "_pga_scan_node": scan,
            }

    return None


# --------------------------------------------------------------------
# Database metadata
# --------------------------------------------------------------------

def load_table_meta(con, schema: str, table: str) -> Optional[TableMeta]:
    """Load relation size and existing indexes for one table."""
    rel_sql = """
        SELECT
            n.nspname,
            c.relname,
            COALESCE(c.reltuples, 0)::float8 AS reltuples,
            COALESCE(c.relpages, 0)::int AS relpages,
            pg_table_size(c.oid)::bigint AS table_bytes
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname = %s
          AND c.relkind IN ('r', 'p')
        LIMIT 1
    """

    idx_sql = """
        SELECT
            idx.relname AS index_name,
            i.indisunique AS is_unique,
            i.indisprimary AS is_primary,
            pg_get_indexdef(i.indexrelid) AS indexdef
        FROM pg_index i
        JOIN pg_class idx ON idx.oid = i.indexrelid
        JOIN pg_class tbl ON tbl.oid = i.indrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        WHERE ns.nspname = %s
          AND tbl.relname = %s
        ORDER BY idx.relname
    """

    with con.cursor() as cur:
        cur.execute(rel_sql, (schema, table))
        row = cur.fetchone()
        if not row:
            return None

        cur.execute(idx_sql, (schema, table))
        indexes = []
        for index_name, is_unique, is_primary, indexdef in cur.fetchall():
            indexes.append(
                {
                    "index_name": index_name,
                    "is_unique": bool(is_unique),
                    "is_primary": bool(is_primary),
                    "indexdef": indexdef,
                    "columns": parse_index_columns(indexdef),
                }
            )

    return TableMeta(
        schema=row[0],
        table=row[1],
        reltuples=float(row[2]),
        relpages=int(row[3]),
        table_bytes=int(row[4]),
        indexes=indexes,
    )


def parse_index_columns(indexdef: str) -> List[str]:
    """
    Extract column names from a simple PostgreSQL index definition.

    Simple definition format:
    CREATE INDEX ... ON schema.table USING btree (col1, col2)
    """
    m = re.search(r"\((.+)\)", indexdef)
    if not m:
        return []

    inside = m.group(1)
    parts = [p.strip() for p in inside.split(",")]

    columns: List[str] = []
    for part in parts:
        cleaned = re.sub(r"\s+(ASC|DESC)\b.*$", "", part, flags=re.IGNORECASE).strip()
        cleaned = cleaned.strip('"')
        columns.append(cleaned)

    return columns


# --------------------------------------------------------------------
# pg_stat_statements context
# --------------------------------------------------------------------

def load_query_stats(con, queryid: int | str, database_name: str = "") -> Optional[QueryStats]:
    """Load pg_stat_statements counters for the analyzed query ID."""
    sql = """
        SELECT
            queryid::text,
            COALESCE(calls, 0)::float8,
            COALESCE(rows, 0)::float8,
            COALESCE(total_exec_time, 0)::float8,
            COALESCE(mean_exec_time, 0)::float8,
            COALESCE(min_exec_time, 0)::float8,
            COALESCE(max_exec_time, 0)::float8,
            COALESCE(stddev_exec_time, 0)::float8,
            COALESCE(shared_blks_hit, 0)::float8,
            COALESCE(shared_blks_read, 0)::float8,
            COALESCE(wal_records, 0)::float8,
            COALESCE(wal_fpi, 0)::float8,
            COALESCE(wal_bytes, 0)::float8
        FROM pg_stat_statements
        WHERE queryid::text = %s
        LIMIT 1
    """
    if not database_name:
        with con.cursor() as cur:
            cur.execute("SELECT current_database()")
            row = cur.fetchone()
            database_name = row[0] if row else ""

    sql = database.prepare_pgss_sql(sql, con, database_name)

    with con.cursor() as cur:
        cur.execute(sql, (str(queryid),))
        row = cur.fetchone()

    if not row:
        return None

    return QueryStats(
        queryid=row[0],
        calls=float(row[1]),
        rows=float(row[2]),
        total_exec_time=float(row[3]),
        mean_exec_time=float(row[4]),
        min_exec_time=float(row[5]),
        max_exec_time=float(row[6]),
        stddev_exec_time=float(row[7]),
        shared_blks_hit=float(row[8]),
        shared_blks_read=float(row[9]),
        wal_records=float(row[10]),
        wal_fpi=float(row[11]),
        wal_bytes=float(row[12]),
    )


# --------------------------------------------------------------------
# pg_stats helpers
# --------------------------------------------------------------------

def parse_pg_array_text(value: Optional[str]) -> Optional[List[str]]:
    """Parse PostgreSQL array text returned by pg_stats into a Python list."""
    if value is None:
        return None

    value = value.strip()
    if not value.startswith("{") or not value.endswith("}"):
        return None

    inner = value[1:-1].strip()
    if not inner:
        return []

    result = []
    current = []
    in_quotes = False
    escape = False

    for ch in inner:
        if escape:
            current.append(ch)
            escape = False
            continue

        if ch == "\\":
            escape = True
            continue

        if ch == '"':
            in_quotes = not in_quotes
            continue

        if ch == "," and not in_quotes:
            result.append("".join(current).strip())
            current = []
            continue

        current.append(ch)

    result.append("".join(current).strip())
    return result


def parse_pg_float_array_text(value: Optional[str]) -> Optional[List[float]]:
    """Parse PostgreSQL array text into floats when all items are numeric."""
    raw = parse_pg_array_text(value)
    if raw is None:
        return None

    out: List[float] = []
    for item in raw:
        try:
            out.append(float(item))
        except (TypeError, ValueError):
            return None
    return out


def load_column_stats(con, schema: str, table: str, column: str) -> Optional[ColumnStats]:
    """Load pg_stats information for one table column."""
    sql = """
        SELECT
            schemaname,
            tablename,
            attname,
            COALESCE(null_frac, 0)::float8,
            COALESCE(n_distinct, 0)::float8,
            most_common_vals::text,
            most_common_freqs::text,
            histogram_bounds::text
        FROM pg_stats
        WHERE schemaname = %s
          AND tablename = %s
          AND attname = %s
        LIMIT 1
    """

    with con.cursor() as cur:
        cur.execute(sql, (schema, table, column))
        row = cur.fetchone()

    if not row:
        return None

    return ColumnStats(
        schema=row[0],
        table=row[1],
        column=row[2],
        null_frac=float(row[3]),
        n_distinct=float(row[4]),
        most_common_vals=parse_pg_array_text(row[5]),
        most_common_freqs=parse_pg_float_array_text(row[6]),
        histogram_bounds=parse_pg_array_text(row[7]),
    )


def build_column_stats_summary(stats: Optional[ColumnStats]) -> str:
    """Format column statistics into a compact explanation string."""
    if stats is None:
        return "n_distinct=unknown"

    parts = [f"n_distinct={stats.n_distinct:g}", f"null_frac={stats.null_frac:.4f}"]

    if stats.most_common_vals:
        parts.append(f"mcv_count={len(stats.most_common_vals)}")
    else:
        parts.append("mcv_count=0")

    if stats.histogram_bounds:
        parts.append(f"histogram_bounds={len(stats.histogram_bounds)}")
    else:
        parts.append("histogram_bounds=0")

    return ", ".join(parts)


def build_candidate_columns_stats_reason(
    con,
    schema: str,
    table: str,
    candidate_columns: List[str],
) -> str:
    """Build a stats summary for a list of candidate index columns."""
    parts: List[str] = []

    for col in candidate_columns:
        stats = load_column_stats(con, schema, table, col)
        parts.append(f"{col}: {build_column_stats_summary(stats)}")

    return " | ".join(parts)


def build_candidate_predicates_stats_reason(
    con,
    schema: str,
    table: str,
    predicates: List[Dict[str, str]],
) -> str:
    """
    Build a statistics summary including the detected operator for each column.
    """
    parts: List[str] = []

    for pred in predicates:
        col = pred["column"]
        op = pred["operator"]
        stats = load_column_stats(con, schema, table, col)
        parts.append(f"{col} [{op}]: {build_column_stats_summary(stats)}")

    return " | ".join(parts)


def try_extract_constant_text(filter_expr: str) -> Optional[str]:
    """Extract a simple literal value from a comparison expression."""
    m = re.search(
        r"""(=|>=|<=|>|<)\s*(?:
            '(?P<quoted>[^']*)'(?:::.*)? |
            (?P<bare>[^\s\)]+)
        )""",
        filter_expr,
        flags=re.IGNORECASE | re.VERBOSE,
    )
    if not m:
        return None

    value = m.group("quoted")
    if value is not None:
        return value.strip()

    value = m.group("bare")
    if value is not None:
        return value.strip()

    return None


def estimate_equality_selectivity_from_stats(
    filter_expr: str,
    column_stats: ColumnStats,
) -> Optional[float]:
    """Estimate equality predicate selectivity from MCV or n_distinct stats."""
    op = extract_simple_operator(filter_expr)
    if op != "=":
        return None

    const_text = try_extract_constant_text(filter_expr)
    if const_text is None:
        return None

    null_frac = column_stats.null_frac or 0.0

    mcv_vals = column_stats.most_common_vals
    mcv_freqs = column_stats.most_common_freqs

    if mcv_vals and mcv_freqs and len(mcv_vals) == len(mcv_freqs):
        for value, freq in zip(mcv_vals, mcv_freqs):
            if str(value) == const_text:
                return max(0.0, min(float(freq), 1.0))

    nd = column_stats.n_distinct
    if nd > 0:
        sel = 1.0 / max(nd, 1.0)
        return max(0.0, min(sel * (1.0 - null_frac), 1.0))

    return None


def try_parse_numeric_constant(filter_expr: str) -> Optional[float]:
    """Extract a numeric literal from a simple comparison expression."""
    m = re.search(
        r"""(=|>=|<=|>|<)\s*(?:'(?P<qnum>-?\d+(?:\.\d+)?)'(?:::.*)?|(?P<num>-?\d+(?:\.\d+)?))""",
        filter_expr,
        flags=re.IGNORECASE | re.VERBOSE,
    )
    if not m:
        return None

    value = m.group("qnum") or m.group("num")
    if value is None:
        return None

    try:
        return float(value)
    except ValueError:
        return None


def extract_simple_operator(filter_expr: str) -> Optional[str]:
    """Return the comparison operator from a simple predicate."""
    m = re.search(r"(=|>=|<=|>|<|~~|LIKE|ILIKE)", filter_expr, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1).upper()


def estimate_selectivity_from_stats(
    filter_expr: str,
    column_stats: ColumnStats,
) -> Optional[float]:
    """Estimate predicate selectivity using pg_stats when the expression is simple."""
    op = extract_simple_operator(filter_expr)
    if not op:
        return None

    if op in {"LIKE", "ILIKE", "~~"}:
        return None

    if op == "=":
        return estimate_equality_selectivity_from_stats(filter_expr, column_stats)

    const = try_parse_numeric_constant(filter_expr)
    if const is None:
        return None

    null_frac = column_stats.null_frac or 0.0

    mcv_vals = column_stats.most_common_vals
    mcv_freqs = column_stats.most_common_freqs

    if mcv_vals and mcv_freqs and len(mcv_vals) == len(mcv_freqs):
        try:
            numeric_mcv_vals = [float(v) for v in mcv_vals]
        except (TypeError, ValueError):
            numeric_mcv_vals = None

        if numeric_mcv_vals is not None:
            matched_freq = 0.0

            for value, freq in zip(numeric_mcv_vals, mcv_freqs):
                if op == ">" and value > const:
                    matched_freq += freq
                elif op == ">=" and value >= const:
                    matched_freq += freq
                elif op == "<" and value < const:
                    matched_freq += freq
                elif op == "<=" and value <= const:
                    matched_freq += freq

            total_mcv_freq = sum(mcv_freqs)
            if total_mcv_freq >= thresholds.RANGE_SELECTIVITY_MIN_MCV_COVERAGE:
                return max(0.0, min(matched_freq, 1.0 - null_frac))

    bounds = column_stats.histogram_bounds
    if not bounds or len(bounds) < 2:
        return None

    try:
        numeric_bounds = [float(x) for x in bounds]
    except (TypeError, ValueError):
        return None

    min_b = numeric_bounds[0]
    max_b = numeric_bounds[-1]

    if max_b <= min_b:
        return None

    if op in {">", ">="}:
        if const <= min_b:
            return 1.0 - null_frac
        if const >= max_b:
            return 0.0
        sel = (max_b - const) / (max_b - min_b)
        return max(0.0, min(sel * (1.0 - null_frac), 1.0))

    if op in {"<", "<="}:
        if const <= min_b:
            return 0.0
        if const >= max_b:
            return 1.0 - null_frac
        sel = (const - min_b) / (max_b - min_b)
        return max(0.0, min(sel * (1.0 - null_frac), 1.0))

    return None


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------

def has_large_row_estimation_gap(
    actual_rows: float,
    plan_rows: float,
    threshold: float = thresholds.DEFAULT_ROW_ESTIMATION_GAP_FACTOR,
) -> tuple[bool, Optional[str]]:
    """
    Detect a significant gap between planner row estimates and actual execution.

    The threshold is a multiplicative factor (e.g. 5 means x5 or /5).
    """
    if plan_rows <= 0:
        return False, None

    if actual_rows < 0:
        return False, None

    ratio = actual_rows / plan_rows

    if ratio >= threshold:
        return True, (
            f"Underestimation (~{ratio:.1f}x more rows than expected)"
        )

    if ratio == 0:
        return True, (
            f"Overestimation (planner expected {plan_rows:.0f} rows, actual rows = 0)"
        )

    if ratio <= 1 / threshold:
        return True, (
            f"Overestimation (~{1/ratio:.1f}x fewer rows than expected)"
        )

    return False, None


def find_index_definition(indexes: List[Dict[str, Any]], index_name: Optional[str]) -> Optional[str]:
    """Return the CREATE INDEX definition for a named existing index."""
    if not index_name:
        return None

    for idx in indexes:
        if idx.get("index_name") == index_name:
            return idx.get("indexdef")
    return None


def compute_row_estimation_ratio(plan_rows: float, actual_rows: float) -> Optional[float]:
    """
    Return a ratio >= 1.0 measuring the mismatch between estimated and actual rows.

    Examples:
      plan=100, actual=100   -> 1.0
      plan=100, actual=1000  -> 10.0
      plan=1000, actual=100  -> 10.0
    """
    if plan_rows < 0 or actual_rows < 0:
        return None

    p = max(plan_rows, 1.0)
    a = max(actual_rows, 1.0)

    return max(a / p, p / a)


def build_row_estimation_reason(plan_rows: float, actual_rows: float) -> str:
    """Explain how far planner row estimates are from actual rows."""
    ratio = compute_row_estimation_ratio(plan_rows, actual_rows)
    if ratio is None:
        return "Row estimate comparison unavailable."

    abs_diff = actual_rows - plan_rows

    if ratio <= thresholds.ROW_ESTIMATION_CLOSE_MAX_FACTOR:
        quality = "Planner row estimate is close to actual rows."
    elif ratio <= thresholds.ROW_ESTIMATION_MODERATE_MAX_FACTOR:
        quality = "Planner row estimate differs moderately from actual rows."
    elif ratio <= thresholds.ROW_ESTIMATION_LARGE_MAX_FACTOR:
        quality = "Planner row estimate differs significantly from actual rows."
    else:
        quality = "Planner row estimate differs very strongly from actual rows."

    return (
        f"{quality} "
        f"plan_rows={plan_rows:.0f}, actual_rows={actual_rows:.0f}, "
        f"absolute_diff={abs_diff:.0f}, mismatch_ratio={ratio:.2f}x."
    )


def is_small_table(meta: TableMeta) -> bool:
    """Return True when an index recommendation is unlikely to be worthwhile."""
    if meta.relpages <= thresholds.SMALL_TABLE_MAX_PAGES:
        return True
    if meta.reltuples > 0 and meta.reltuples <= thresholds.SMALL_TABLE_MAX_ROWS:
        return True
    if meta.table_bytes <= thresholds.SMALL_TABLE_MAX_BYTES:
        return True
    return False




def is_full_relation_scan_without_predicate(finding: ScanFinding, meta: TableMeta) -> bool:
    """
    True when the plan is intentionally reading most/all rows. In this case a
    plain btree index cannot reduce the scan volume, so the advisor should stay
    quiet instead of producing a noisy non-actionable recommendation.
    """
    if finding.filter_expr or finding.index_cond or finding.recheck_cond:
        return False

    if meta.reltuples <= 0:
        return finding.rows_removed_by_filter <= 0

    # Prefer actuals when the node executed; otherwise fall back to Plan Rows.
    rows = finding.actual_rows if finding.actual_loops > 0 else finding.plan_rows
    if rows <= 0:
        return False

    return (rows / meta.reltuples) >= thresholds.FULL_SCAN_MIN_TABLE_FRACTION


def build_no_filter_seq_scan_reason(finding: ScanFinding, meta: TableMeta) -> str:
    """Explain why a sequential scan without a visible predicate is not actionable."""
    relation = f'{finding.schema}.{finding.table}'

    if is_small_table(meta):
        return (
            f"Sequential scan on small table {relation}: no selective predicate is visible, "
            "and scanning the table is usually cheaper than using an index."
        )

    if is_full_relation_scan_without_predicate(finding, meta):
        parent = f" under {finding.parent_node_type}" if finding.parent_node_type else ""
        return (
            f"Full sequential scan on {relation}{parent}: no WHERE predicate is available "
            "on this relation, so a normal index would not reduce the number of rows read. "
            "This is expected for full joins/aggregations; no index recommendation is emitted "
            "for this scan node."
        )

    return (
        f"Sequential scan on {relation} without a visible filter. No safe index recommendation "
        "can be derived from this scan node alone."
    )

def estimate_selected_fraction(finding: ScanFinding) -> Optional[float]:
    """Estimate how selective a filter was from actual rows and removed rows."""
    scanned_rows = finding.actual_rows + finding.rows_removed_by_filter
    if scanned_rows <= 0:
        return None
    return min(finding.actual_rows / scanned_rows, 1.0)


def estimate_planned_selected_fraction(finding: ScanFinding, meta: TableMeta) -> Optional[float]:
    """
    Fallback when a scan node was planned but not executed, e.g. inner side of a
    Nested Loop with Actual Loops = 0. EXPLAIN ANALYZE has no actual filtered
    rows for that node, but Plan Rows vs reltuples still tells us whether the
    planner expected selective predicates.
    """
    if meta.reltuples <= 0 or finding.plan_rows < 0:
        return None
    return max(0.0, min(float(finding.plan_rows) / float(meta.reltuples), 1.0))


def is_planned_scan_potentially_expensive(finding: ScanFinding, meta: TableMeta) -> bool:
    """
    Cost-side guard for nodes that were not executed. This prevents the advisor
    from saying "already fast" solely because Actual Loops = 0.
    """
    if finding.actual_loops > 0:
        return False

    if finding.total_cost >= thresholds.PLANNED_SCAN_MIN_COST:
        return True

    # Relative fallback for small databases where absolute costs are low.
    if (
        meta.relpages > thresholds.SMALL_TABLE_MAX_PAGES
        and finding.total_cost >= max(
            thresholds.PLANNED_SCAN_MIN_RELATIVE_COST,
            meta.relpages * thresholds.PLANNED_SCAN_COST_PER_PAGE,
        )
    ):
        return True

    return False


def strip_outer_parentheses(expr: str) -> str:
    """Remove balanced outer parentheses from an expression string."""
    expr = expr.strip()

    while expr.startswith("(") and expr.endswith(")"):
        depth = 0
        balanced_outer = True

        for i, ch in enumerate(mask_sql_quotes(expr)):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1

            if depth == 0 and i < len(expr) - 1:
                balanced_outer = False
                break

        if not balanced_outer:
            break

        expr = expr[1:-1].strip()

    return expr


def mask_sql_quotes(expr: str) -> str:
    """Hide quoted contents while preserving offsets for structural parsing.

    Backslash escapes and dollar-quoted strings are deliberately unsupported;
    callers reject them rather than guessing their SQL semantics.
    """
    return re.sub(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"",
                  lambda match: " " * len(match.group()), expr)


def split_top_level_boolean(expr: str, operator: str) -> List[str]:
    """Split boolean operators outside parentheses, brackets and quoted text."""
    masked = mask_sql_quotes(expr)
    depth = 0
    start = 0
    parts = []
    for match in re.finditer(r"[()\[\]]|\b" + operator + r"\b", masked, re.IGNORECASE):
        token = match.group()
        if token in ("(", "["):
            depth += 1
        elif token in (")", "]"):
            depth -= 1
        elif depth == 0:
            parts.append(expr[start:match.start()].strip())
            start = match.end()
    parts.append(expr[start:].strip())
    return parts


def split_top_level_and(expr: str) -> List[str]:
    """Split top-level conjunctions without splitting an OR expression."""
    if len(split_top_level_boolean(expr, "OR")) > 1:
        return [expr]
    return split_top_level_boolean(expr, "AND")


def strip_trivial_lhs_casts(expr: str) -> str:
    """Remove simple casts around the left side of a predicate."""
    expr = expr.strip()

    while True:
        expr = strip_outer_parentheses(expr)

        m = re.match(
            r'^\((?P<inner>.+)\)\s*::\s*[A-Za-z0-9_\[\]\."]+$',
            expr,
            flags=re.IGNORECASE,
        )
        if not m:
            break

        expr = m.group("inner").strip()

    expr = strip_outer_parentheses(expr)
    return expr


_SQL_IDENTIFIER = r'(?:"(?:[^"\n]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)'
_SQL_COLUMN = rf'(?:(?P<prefix>{_SQL_IDENTIFIER})\s*\.\s*)?(?P<col>{_SQL_IDENTIFIER})'
_SQL_CAST = r'(?:::\s*[A-Za-z_][A-Za-z0-9_.]*(?:\[\])?)*'
_SQL_VALUE = rf"(?:'(?:[^']|'')*'|[-+]?\d+(?:\.\d+)?|\$\d+|TRUE|FALSE|NULL){_SQL_CAST}"


def _decode_identifier(value: str) -> str:
    return value[1:-1].replace('""', '"') if value.startswith('"') else value.lower()


def parse_simple_filter_predicates(
    filter_expr: str, alias: Optional[str], table: str,
) -> tuple[List[Dict[str, str]], bool]:
    """Return supported conjuncts and whether the entire filter was understood.

    OR branches are never treated as mandatory predicates. Unsupported AND
    clauses may be skipped, but their selectivity cannot be attributed to the
    retained columns. IN/ANY are multi-value searches, not single equalities.
    """
    expr = strip_outer_parentheses(filter_expr.strip())
    if "\\" in expr or re.search(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$", expr):
        return [], False
    if len(split_top_level_boolean(expr, "OR")) > 1:
        return [], False
    clauses = split_top_level_and(expr)
    if len(clauses) > 1:
        predicates = []
        complete = True
        for clause in clauses:
            parsed, supported = parse_simple_filter_predicates(clause, alias, table)
            predicates = merge_simple_predicates(predicates, parsed)
            complete = complete and supported
        return predicates, complete

    # Find the operator outside quotes; validate both operands in full below.
    match = re.search(r"\bIS\s+NOT\s+NULL\b|\bIS\s+NULL\b|\bIN\b|>=|<=|=|>|<|~~|\bLIKE\b",
                      mask_sql_quotes(expr), re.IGNORECASE)
    if not match:
        return [], False
    lhs = strip_trivial_lhs_casts(expr[:match.start()].strip())
    column = re.fullmatch(_SQL_COLUMN, lhs)
    if not column:
        return [], False
    prefix = column.group("prefix")
    if prefix and _decode_identifier(prefix) != (alias or table):
        return [], False
    operator = " ".join(match.group().upper().split())
    rhs = expr[match.end():].strip()
    if operator in {"IS NULL", "IS NOT NULL"}:
        supported = not rhs
    elif operator == "IN":
        supported = bool(re.fullmatch(rf"\(\s*{_SQL_VALUE}(?:\s*,\s*{_SQL_VALUE})*\s*\)", rhs, re.IGNORECASE))
    elif operator == "=" and re.match(r"ANY\s*\(", rhs, re.IGNORECASE):
        operator = "ANY"
        array_value = rf"(?:'(?:[^']|'')*'|\$\d+|ARRAY\s*\[\s*{_SQL_VALUE}(?:\s*,\s*{_SQL_VALUE})*\s*\]){_SQL_CAST}"
        supported = bool(re.fullmatch(rf"ANY\s*\(\s*{array_value}\s*\)", rhs, re.IGNORECASE))
    else:
        supported = bool(re.fullmatch(_SQL_VALUE, rhs, re.IGNORECASE))
        # A parameterized nested-loop condition can reference an outer alias.
        outer_column = re.fullmatch(_SQL_COLUMN, rhs)
        if outer_column and outer_column.group("prefix"):
            supported = _decode_identifier(outer_column.group("prefix")) != (alias or table)
    if not supported:
        return [], False
    return [{"column": _decode_identifier(column.group("col")), "operator": operator}], True


def extract_simple_filter_columns(filter_expr: str, alias: Optional[str], table: str) -> List[str]:
    """Return columns from supported, mandatory filter predicates."""
    return [p["column"] for p in extract_simple_filter_predicates(filter_expr, alias, table)]


def extract_simple_filter_predicates(
    filter_expr: str, alias: Optional[str], table: str,
) -> List[Dict[str, str]]:
    """Extract comparisons, null checks and simple IN/ANY searches from ANDs."""
    return parse_simple_filter_predicates(filter_expr, alias, table)[0]


def _operator_rank_for_btree(op: str) -> int:
    """
    Rank operators to order columns in a composite B-tree index:
    0 -> equality or IS NULL
    1 -> multi-value IN/ANY or prefix LIKE / ~~ (conditional)
    2 -> range or IS NOT NULL
    9 -> fallback
    """
    op = (op or "").upper()

    if op in {"=", "IS NULL"}:
        return 0
    if op in {"LIKE", "~~", "IN", "ANY"}:
        return 1
    if op in {">", ">=", "<", "<=", "IS NOT NULL"}:
        return 2
    return 9


def _column_cardinality_score(
    stats: Optional[ColumnStats],
    table_rows: Optional[float] = None,
) -> float:
    """
    Higher scores indicate more discriminating columns.
    Heuristic:
    - n_distinct > 0: estimated absolute cardinality
    - n_distinct < 0: fraction of distinct rows (pg_stats convention)
    """
    if stats is None:
        return 0.0

    nd = stats.n_distinct

    if nd > 0:
        return float(nd)

    if nd < 0:
        distinct_fraction = abs(float(nd))
        if table_rows and table_rows > 0:
            return distinct_fraction * float(table_rows)
        return distinct_fraction

    return 0.0


def reorder_index_candidate_columns(
    con,
    schema: str,
    table: str,
    predicates: List[Dict[str, str]],
    table_rows: Optional[float] = None,
) -> List[str]:
    """
    Reorders candidate columns for a composite index:
    - equality and IS NULL columns first
    - then IN/ANY searches and LIKE / ~~ prefix predicates
    - then range and IS NOT NULL predicates
    - within each group: descending estimated cardinality
    """
    if not predicates:
        return []

    scored: List[tuple[int, float, int, str]] = []

    for pos, pred in enumerate(predicates):
        col = pred["column"]
        op = pred["operator"]
        stats = load_column_stats(con, schema, table, col)

        op_rank = _operator_rank_for_btree(op)
        cardinality = _column_cardinality_score(stats, table_rows)

        # Sort by operator ascending, cardinality descending, then original position.
        scored.append((op_rank, -cardinality, pos, col))

    scored.sort()

    ordered: List[str] = []
    for _, _, _, col in scored:
        if col not in ordered:
            ordered.append(col)

    return ordered


def extract_simple_join_columns(
    cond_expr: str,
) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Extract alias/column pairs from a simple equality join condition."""
    expr = strip_outer_parentheses(cond_expr.strip())

    m = re.match(
        r'^(?P<left_alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<left_col>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*'
        r'(?P<right_alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<right_col>[A-Za-z_][A-Za-z0-9_]*)$',
        expr,
        flags=re.IGNORECASE,
    )
    if not m:
        return None, None, None, None

    return (
        m.group("left_alias"),
        m.group("left_col"),
        m.group("right_alias"),
        m.group("right_col"),
    )



def merge_simple_predicates(
    first: List[Dict[str, str]],
    second: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    """
    Merges two predicate lists while keeping the first predicate encountered
    for each column. Useful for combining Index Cond + Filter without duplicating
    a column already constrained by the current index.
    """
    merged: List[Dict[str, str]] = []
    seen = set()

    for pred in (first or []) + (second or []):
        col = pred.get("column")
        op = pred.get("operator")
        if not col or not op or col in seen:
            continue
        merged.append({"column": col, "operator": op})
        seen.add(col)

    return merged


def find_index_columns(indexes: List[Dict[str, Any]], index_name: Optional[str]) -> List[str]:
    """Return parsed columns for an existing index name."""
    if not index_name:
        return []

    for idx in indexes:
        if idx.get("index_name") == index_name:
            return [c.strip('"') for c in idx.get("columns", [])]
    return []


def candidate_adds_columns_to_used_index(
    used_index_columns: List[str],
    candidate_columns: List[str],
) -> bool:
    """
    Returns True if the candidate contains at least one column missing from the current index.
    Exact column ordering is not required because reorder_index_candidate_columns
    may intentionally move equality predicates before range predicates.
    """
    if not candidate_columns:
        return False

    used = {c.strip('"') for c in used_index_columns or []}
    candidate = [c.strip('"') for c in candidate_columns]

    return any(c not in used for c in candidate)


def candidate_improves_index_key_order(
    used_columns: List[str],
    candidate_columns: List[str],
    predicates: List[Dict[str, str]],
    filter_predicates: List[Dict[str, str]],
) -> bool:
    """Recognize useful equality promotion, not arbitrary cardinality reordering."""
    equality_columns = {p["column"] for p in predicates if p["operator"] == "="}
    for predicate in filter_predicates:
        column = predicate["column"]
        if predicate["operator"] != "=" or column not in used_columns or column not in candidate_columns:
            continue
        old_prefix = used_columns[:used_columns.index(column)]
        new_prefix = candidate_columns[:candidate_columns.index(column)]
        if any(key not in equality_columns for key in old_prefix) and all(
            key in equality_columns for key in new_prefix
        ):
            return True
    return False


def estimate_post_index_filter_fraction(finding: ScanFinding) -> Optional[float]:
    """
    Return the fraction of rows kept after residual filtering on an indexed path.
    Lower values mean that more tuples retrieved through the current index
    are subsequently discarded by the executor.
    """
    visited_after_index = finding.actual_rows + finding.rows_removed_by_filter
    if visited_after_index <= 0:
        return None
    return min(finding.actual_rows / visited_after_index, 1.0)


def build_post_index_filter_reason(finding: ScanFinding) -> Optional[str]:
    """Explain how much residual filtering happened after an indexed access."""
    fraction = estimate_post_index_filter_fraction(finding)
    if fraction is None:
        return None

    removed = finding.rows_removed_by_filter
    kept = finding.actual_rows
    return (
        f"Residual filter kept {fraction:.1%} of tuples visited by the indexed path "
        f"(actual_rows={kept:.0f}, rows_removed_by_filter={removed:.0f})."
    )

def looks_like_prefix_search(filter_expr: str) -> bool:
    """Detect LIKE 'prefix%' predicates that may need operator-class review."""
    return bool(
        re.search(r"(LIKE|~~)\s+'[^%_']+%'", filter_expr, flags=re.IGNORECASE)
    )


def looks_suspicious_predicate(filter_expr: str) -> bool:
    """Flag predicates that look semantically unusual and need human review."""
    expr = filter_expr.lower()
    if "discount" in expr and "> '1'" in expr:
        return True
    if "discount" in expr and "> 1" in expr:
        return True
    return False


def extract_simple_sort_keys(
    keys: Optional[List[str]],
    alias: Optional[str],
    table: str,
) -> List[Dict[str, str]]:
    """
    Extracts simple ORDER BY keys from PostgreSQL JSON plan Sort Key entries.
    Only plain columns are accepted. Expressions, functions, CASE, and mixed
    relation keys intentionally return an empty list.

    Returns: [{"column": "created_at", "direction": "DESC"}]
    """
    if not keys:
        return []

    result: List[Dict[str, str]] = []
    seen = set()

    for raw_key in keys:
        key = str(raw_key).strip()

        direction = "ASC"
        if re.search(r"\bDESC\b", key, flags=re.IGNORECASE):
            direction = "DESC"

        key = re.sub(
            r"\s+(ASC|DESC)\b.*$",
            "",
            key,
            flags=re.IGNORECASE,
        ).strip()
        key = strip_trivial_lhs_casts(key)
        key = key.strip('"')

        m = re.match(
            r'^(?:(?P<prefix>[A-Za-z_][A-Za-z0-9_]*)\.)?(?P<col>[A-Za-z_][A-Za-z0-9_]*)$',
            key,
            flags=re.IGNORECASE,
        )
        if not m:
            return []

        found_prefix = m.group("prefix")
        col = m.group("col")

        if alias and found_prefix and found_prefix != alias:
            return []

        if not alias and found_prefix and found_prefix != table:
            return []

        if col in seen:
            continue

        result.append({"column": col, "direction": direction})
        seen.add(col)

    return result


def extract_simple_group_keys(
    keys: Optional[List[str]],
    alias: Optional[str],
    table: str,
) -> List[str]:
    """Extract GROUP BY columns using the same parser as ORDER BY keys."""
    order_keys = extract_simple_sort_keys(keys, alias=alias, table=table)
    return [item["column"] for item in order_keys]


def merge_columns_with_order(
    filter_columns: List[str],
    order_columns: List[Dict[str, str]],
) -> List[str]:
    """Merge equality/filter columns with ordered columns for composite indexes."""
    merged: List[str] = []

    for col in filter_columns or []:
        if col not in merged:
            merged.append(col)

    for item in order_columns or []:
        col = item.get("column")
        if col and col not in merged:
            merged.append(col)

    return merged


def build_create_index_sql_with_order(
    schema: str,
    table: str,
    filter_columns: List[str],
    order_columns: List[Dict[str, str]],
) -> str:
    """Build CREATE INDEX SQL preserving ORDER BY directions where needed."""
    index_parts: List[str] = []
    name_parts: List[str] = []

    for col in filter_columns or []:
        if col not in name_parts:
            index_parts.append(quote_identifier(col))
            name_parts.append(col)

    for item in order_columns or []:
        col = item.get("column")
        if not col or col in name_parts:
            continue

        direction = str(item.get("direction", "ASC")).upper()
        if direction not in {"ASC", "DESC"}:
            direction = "ASC"

        index_parts.append(f'{quote_identifier(col)} {direction}')
        name_parts.append(f"{col}_{direction.lower()}")

    idx_name = f"pga_idx_{table}_{'_'.join(name_parts)}"
    cols_sql = ", ".join(index_parts)
    return f'CREATE INDEX CONCURRENTLY {quote_identifier(idx_name)} ON {quote_identifier(schema)}.{quote_identifier(table)} ({cols_sql});'


def sort_spilled_to_disk(finding: OrderByFinding) -> bool:
    """Return True when PostgreSQL reported a disk-backed sort."""
    return (finding.sort_space_type or "").lower() == "disk"


def build_sort_context_reason(finding: OrderByFinding) -> str:
    """Build a compact ORDER BY context string for recommendation reasons."""
    details: List[str] = []

    if finding.has_limit:
        details.append("ORDER BY is directly under a LIMIT")

    if finding.sort_key:
        details.append(f"sort_key={finding.sort_key}")

    if finding.sort_method:
        details.append(f"sort_method={finding.sort_method}")

    if finding.sort_space_type:
        space = f", sort_space_used={finding.sort_space_used:g}kB" if finding.sort_space_used is not None else ""
        details.append(f"sort_space_type={finding.sort_space_type}{space}")

    if finding.actual_total_time:
        details.append(f"sort_actual_total_time={finding.actual_total_time:.3f} ms")

    return "; ".join(details)


def group_by_spilled_to_disk(finding: GroupByFinding) -> bool:
    """Return True when the GROUP BY supporting sort spilled to disk."""
    return (finding.sort_space_type or "").lower() == "disk"


def build_group_by_context_reason(finding: GroupByFinding) -> str:
    """Build a compact GROUP BY context string for recommendation reasons."""
    details: List[str] = []

    if finding.strategy:
        details.append(f"strategy={finding.strategy}")

    if finding.group_key:
        details.append(f"group_key={finding.group_key}")

    if finding.sort_method:
        details.append(f"sort_method={finding.sort_method}")

    if finding.sort_space_type:
        space = f", sort_space_used={finding.sort_space_used:g}kB" if finding.sort_space_used is not None else ""
        details.append(f"sort_space_type={finding.sort_space_type}{space}")

    if finding.actual_total_time:
        details.append(f"group_actual_total_time={finding.actual_total_time:.3f} ms")

    return "; ".join(details)


def find_equivalent_index(indexes: List[Dict[str, Any]], candidate_columns: List[str]) -> Optional[str]:
    """Return an existing index whose leading columns cover the candidate."""
    wanted = [c.strip('"') for c in candidate_columns]

    for idx in indexes:
        cols = [c.strip('"') for c in idx.get("columns", [])]
        if cols[: len(wanted)] == wanted:
            return idx["index_name"]
    return None


def quote_identifier(value: str) -> str:
    """Quote a PostgreSQL identifier, including embedded double quotes."""
    return '"' + value.replace('"', '""') + '"'


def build_create_index_sql(
    schema: str, table: str, columns: List[str],
    existing_indexes: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Build a simple concurrent B-tree index creation statement."""
    base_name = f"pga_idx_{table}_{'_'.join(columns)}"
    # PostgreSQL identifiers are limited to 63 bytes. Keep the suffix within
    # that limit, including when multibyte identifiers are used.
    idx_name = base_name.encode("utf-8")[:63].decode("utf-8", errors="ignore")
    existing_names = {idx.get("index_name") for idx in existing_indexes or []}
    suffix_number = 2
    while idx_name in existing_names:
        suffix = f"_{suffix_number}"
        idx_name = base_name.encode("utf-8")[:63 - len(suffix)].decode("utf-8", errors="ignore") + suffix
        suffix_number += 1
    cols_sql = ", ".join(quote_identifier(c) for c in columns)
    return f'CREATE INDEX CONCURRENTLY {quote_identifier(idx_name)} ON {quote_identifier(schema)}.{quote_identifier(table)} ({cols_sql});'


def is_high_workload(query_stats: Optional[QueryStats]) -> bool:
    """Return True when pg_stat_statements says the query is significant."""
    if not query_stats:
        return False
    return (
        query_stats.calls >= thresholds.HIGH_WORKLOAD_MIN_CALLS
        or query_stats.total_exec_time >= thresholds.HIGH_WORKLOAD_MIN_TOTAL_EXEC_TIME_MS
        or query_stats.mean_exec_time >= thresholds.HIGH_WORKLOAD_MIN_MEAN_EXEC_TIME_MS
    )
