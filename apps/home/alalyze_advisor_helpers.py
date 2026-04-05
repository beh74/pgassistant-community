import json
import re
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

from . import database


# --------------------------------------------------------------------
# Data classes
# --------------------------------------------------------------------

@dataclass
class TableMeta:
    schema: str
    table: str
    reltuples: float
    relpages: int
    table_bytes: int
    indexes: List[Dict[str, Any]]


@dataclass
class ScanFinding:
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
    rows_removed_by_filter: float
    shared_hit_blocks: int
    shared_read_blocks: int
    actual_total_time: float
    index_def: Optional[str] = None


@dataclass
class JoinFinding:
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
class QueryStats:
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
    schema: str
    table: str
    confidence: str
    reason: str
    filter_expr: Optional[str] = None
    candidate_columns: Optional[List[str]] = None
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
    if "db_config" in session:
        return session["db_config"]
    if "database" in session:
        return session["database"]
    return session


# --------------------------------------------------------------------
# JSON plan parsing
# --------------------------------------------------------------------

def normalize_plan_json(plan_json: Any) -> Any:
    if isinstance(plan_json, str):
        return json.loads(plan_json)
    return plan_json


def extract_root_plan(plan_json: Any) -> Dict[str, Any]:
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
) -> None:
    node_type = node.get("Node Type", "")

    # ------------------------------------------------------------
    # Scan findings
    # ------------------------------------------------------------
    if node_type in {"Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan"} \
       and node.get("Relation Name") and node.get("Schema"):
        scan_findings.append(
            ScanFinding(
                schema=node["Schema"],
                table=node["Relation Name"],
                alias=node.get("Alias"),
                node_type=node_type,
                index_name=node.get("Index Name"),
                index_cond=node.get("Index Cond"),
                recheck_cond=node.get("Recheck Cond"),
                filter_expr=node.get("Filter"),
                actual_rows=float(node.get("Actual Rows", 0) or 0),
                plan_rows=float(node.get("Plan Rows", 0) or 0),
                actual_loops=float(node.get("Actual Loops", 0) or 0),
                rows_removed_by_filter=float(node.get("Rows Removed by Filter", 0) or 0),
                shared_hit_blocks=int(node.get("Shared Hit Blocks", 0) or 0),
                shared_read_blocks=int(node.get("Shared Read Blocks", 0) or 0),
                actual_total_time=float(node.get("Actual Total Time", 0) or 0),
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

    for child in node.get("Plans", []) or []:
        walk_plan_collect_findings(child, scan_findings, join_findings)


# --------------------------------------------------------------------
# Database metadata
# --------------------------------------------------------------------

def load_table_meta(con, schema: str, table: str) -> Optional[TableMeta]:
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
    Parse très simple :
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

def load_query_stats(con, queryid: int | str) -> Optional[QueryStats]:
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
    parts: List[str] = []

    for col in candidate_columns:
        stats = load_column_stats(con, schema, table, col)
        parts.append(f"{col}: {build_column_stats_summary(stats)}")

    return " | ".join(parts)


def try_extract_constant_text(filter_expr: str) -> Optional[str]:
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
    m = re.search(r"(=|>=|<=|>|<|~~|LIKE|ILIKE)", filter_expr, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1).upper()


def estimate_selectivity_from_stats(
    filter_expr: str,
    column_stats: ColumnStats,
) -> Optional[float]:
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
            if total_mcv_freq >= 0.80:
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
    threshold: float = 5.0,
) -> tuple[bool, Optional[str]]:
    """
    Détecte un écart significatif entre estimation planner et exécution réelle.

    threshold = facteur multiplicatif (ex: 5 => x5 ou /5)
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
    if not index_name:
        return None

    for idx in indexes:
        if idx.get("index_name") == index_name:
            return idx.get("indexdef")
    return None

def compute_row_estimation_ratio(plan_rows: float, actual_rows: float) -> Optional[float]:
    """
    Retourne un ratio >= 1.0 représentant l'écart absolu entre estimation et réel.

    Exemples:
      plan=100, actual=100   -> 1.0
      plan=100, actual=1000  -> 10.0
      plan=1000, actual=100  -> 10.0
    """
    if plan_rows < 0 or actual_rows < 0:
        return None

    # éviter division par zéro tout en restant interprétable
    p = max(plan_rows, 1.0)
    a = max(actual_rows, 1.0)

    return max(a / p, p / a)

def build_row_estimation_reason(plan_rows: float, actual_rows: float) -> str:
    ratio = compute_row_estimation_ratio(plan_rows, actual_rows)
    if ratio is None:
        return "Row estimate comparison unavailable."

    abs_diff = actual_rows - plan_rows

    if ratio <= 1.5:
        quality = "Planner row estimate is close to actual rows."
    elif ratio <= 3.0:
        quality = "Planner row estimate differs moderately from actual rows."
    elif ratio <= 10.0:
        quality = "Planner row estimate differs significantly from actual rows."
    else:
        quality = "Planner row estimate differs very strongly from actual rows."

    return (
        f"{quality} "
        f"plan_rows={plan_rows:.0f}, actual_rows={actual_rows:.0f}, "
        f"absolute_diff={abs_diff:.0f}, mismatch_ratio={ratio:.2f}x."
    )




def is_small_table(meta: TableMeta) -> bool:
    if meta.relpages <= 8:
        return True
    if meta.reltuples > 0 and meta.reltuples <= 1000:
        return True
    if meta.table_bytes <= 128 * 1024:
        return True
    return False


def estimate_selected_fraction(finding: ScanFinding) -> Optional[float]:
    scanned_rows = finding.actual_rows + finding.rows_removed_by_filter
    if scanned_rows <= 0:
        return None
    return min(finding.actual_rows / scanned_rows, 1.0)


def strip_outer_parentheses(expr: str) -> str:
    expr = expr.strip()

    while expr.startswith("(") and expr.endswith(")"):
        depth = 0
        balanced_outer = True

        for i, ch in enumerate(expr):
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


def split_top_level_and(expr: str) -> List[str]:
    parts: List[str] = []
    current: List[str] = []
    depth = 0
    i = 0
    upper_expr = expr.upper()

    while i < len(expr):
        ch = expr[i]

        if ch == "(":
            depth += 1
            current.append(ch)
            i += 1
            continue

        if ch == ")":
            depth -= 1
            current.append(ch)
            i += 1
            continue

        if depth == 0 and upper_expr[i:i + 3] == "AND":
            prev_ok = (i == 0) or expr[i - 1].isspace() or expr[i - 1] == ")"
            next_ok = (i + 3 >= len(expr)) or expr[i + 3].isspace() or expr[i + 3] == "("

            if prev_ok and next_ok:
                part = "".join(current).strip()
                if part:
                    parts.append(part)
                current = []
                i += 3
                continue

        current.append(ch)
        i += 1

    tail = "".join(current).strip()
    if tail:
        parts.append(tail)

    return parts


def strip_trivial_lhs_casts(expr: str) -> str:
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


def extract_simple_filter_columns(filter_expr: str, alias: Optional[str], table: str) -> List[str]:
    expr = strip_outer_parentheses(filter_expr.strip())
    clauses = split_top_level_and(expr)

    cols: List[str] = []
    op_pattern = re.compile(r"\s*(=|>=|<=|>|<|~~|LIKE|ILIKE)\s*", flags=re.IGNORECASE)

    for clause in clauses:
        clause = strip_outer_parentheses(clause)

        m = op_pattern.search(clause)
        if not m:
            return []

        lhs = clause[:m.start()].strip()
        op = m.group(1).upper()

        lhs = strip_trivial_lhs_casts(lhs)

        lhs_match = re.match(
            r'^(?:(?P<prefix>[A-Za-z_][A-Za-z0-9_]*)\.)?(?P<col>[A-Za-z_][A-Za-z0-9_]*)$',
            lhs,
            flags=re.IGNORECASE,
        )
        if not lhs_match:
            return []

        found_prefix = lhs_match.group("prefix")
        col = lhs_match.group("col")

        if alias and found_prefix and found_prefix != alias:
            return []

        if not alias and found_prefix and found_prefix != table:
            return []

        if op == "ILIKE":
            return []

        cols.append(col)

    deduped: List[str] = []
    for c in cols:
        if c not in deduped:
            deduped.append(c)

    return deduped


def extract_simple_join_columns(
    cond_expr: str,
) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
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


def looks_like_prefix_search(filter_expr: str) -> bool:
    return bool(
        re.search(r"(LIKE|~~)\s+'[^%_']+%'", filter_expr, flags=re.IGNORECASE)
    )


def looks_suspicious_predicate(filter_expr: str) -> bool:
    expr = filter_expr.lower()
    if "discount" in expr and "> '1'" in expr:
        return True
    if "discount" in expr and "> 1" in expr:
        return True
    return False


def find_equivalent_index(indexes: List[Dict[str, Any]], candidate_columns: List[str]) -> Optional[str]:
    wanted = [c.strip('"') for c in candidate_columns]

    for idx in indexes:
        cols = [c.strip('"') for c in idx.get("columns", [])]
        if cols[: len(wanted)] == wanted:
            return idx["index_name"]
    return None


def build_create_index_sql(schema: str, table: str, columns: List[str]) -> str:
    idx_name = f"pga_idx_{table}_{'_'.join(columns)}"
    cols_sql = ", ".join(f'"{c}"' for c in columns)
    return f'CREATE INDEX CONCURRENTLY "{idx_name}" ON "{schema}"."{table}" ({cols_sql});'


def is_high_workload(query_stats: Optional[QueryStats]) -> bool:
    if not query_stats:
        return False
    return (
        query_stats.calls >= 1000
        or query_stats.total_exec_time >= 5000
        or query_stats.mean_exec_time >= 5
    )
