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
    filter_expr: Optional[str]
    actual_rows: float
    plan_rows: float
    actual_loops: float
    rows_removed_by_filter: float
    shared_hit_blocks: int
    shared_read_blocks: int
    actual_total_time: float


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
    confidence: str  # "safe" | "review" | "none"
    reason: str
    filter_expr: Optional[str] = None
    candidate_columns: Optional[List[str]] = None
    create_index_sql: Optional[str] = None
    existing_index_match: Optional[str] = None
    stats_reason: Optional[str] = None


# --------------------------------------------------------------------
# Public entry point
# --------------------------------------------------------------------

def analyze_plan_for_safe_indexes(
    plan_json: Any,
    session: Dict[str, Any],
    queryid: int | str | None = None,
) -> Dict[str, Any]:
    """
    Analyse un plan EXPLAIN ANALYZE FORMAT JSON et propose des index "safe only".

    Parameters
    ----------
    plan_json:
        Structure Python issue du JSON ou string JSON.
    session:
        Session applicative permettant d'extraire db_config.
    queryid:
        Optionnel. Permet de récupérer le contexte pg_stat_statements.

    Returns
    -------
    dict
    """
    db_config = get_db_config_from_session(session)
    con, message = database.connectdb(db_config)

    if con is None:
        return {
            "ok": False,
            "message": message or "Unable to connect to database.",
            "recommendations": [],
            "scan_findings": [],
            "query_stats": None,
        }

    try:
        parsed_plan = normalize_plan_json(plan_json)
        root = extract_root_plan(parsed_plan)

        findings: List[ScanFinding] = []
        walk_plan_for_seq_scans(root, findings)

        query_stats = load_query_stats(con, queryid) if queryid is not None else None

        recommendations: List[Recommendation] = []

        for finding in findings:
            meta = load_table_meta(con, finding.schema, finding.table)
            if meta is None:
                recommendations.append(
                    Recommendation(
                        schema=finding.schema,
                        table=finding.table,
                        confidence="none",
                        reason="Could not load table metadata.",
                        filter_expr=finding.filter_expr,
                    )
                )
                continue

            rec = evaluate_seq_scan_candidate(con, finding, meta, query_stats)
            recommendations.append(rec)

        return {
            "ok": True,
            "message": "Plan analyzed successfully.",
            "recommendations": [asdict(r) for r in recommendations],
            "scan_findings": [asdict(f) for f in findings],
            "query_stats": asdict(query_stats) if query_stats else None,
        }

    finally:
        try:
            con.close()
        except Exception:
            pass


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


def walk_plan_for_seq_scans(node: Dict[str, Any], findings: List[ScanFinding]) -> None:
    node_type = node.get("Node Type", "")

    if node_type == "Seq Scan" and node.get("Relation Name") and node.get("Schema"):
        findings.append(
            ScanFinding(
                schema=node["Schema"],
                table=node["Relation Name"],
                alias=node.get("Alias"),
                node_type=node_type,
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

    for child in node.get("Plans", []) or []:
        walk_plan_for_seq_scans(child, findings)


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
    """
    Parse simple d'un tableau PostgreSQL renvoyé sous forme texte.
    Ex:
      "{0.1,0.2,0.3}"
      "{\"A\",\"B\"}"
    """
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


def try_parse_numeric_constant(filter_expr: str) -> Optional[float]:
    """
    Extrait une constante numérique simple depuis:
      col > '0.25'::double precision
      col > 0.25
      col >= 10
    """
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
    """
    Estimation prudente et partielle.
    Supporte:
      col = constant
      col > constant
      col >= constant
      col < constant
      col <= constant

    Priorité:
      1. most_common_vals + most_common_freqs
      2. histogram_bounds
      3. n_distinct pour "="
    """
    op = extract_simple_operator(filter_expr)
    if not op:
        return None

    if op in {"LIKE", "ILIKE", "~~"}:
        return None

    const = try_parse_numeric_constant(filter_expr)
    if const is None:
        return None

    null_frac = column_stats.null_frac or 0.0

    # ----------------------------------------------------------------
    # 1) Utiliser les MCV si disponibles
    # ----------------------------------------------------------------
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
                if op == "=" and value == const:
                    matched_freq += freq
                elif op == ">" and value > const:
                    matched_freq += freq
                elif op == ">=" and value >= const:
                    matched_freq += freq
                elif op == "<" and value < const:
                    matched_freq += freq
                elif op == "<=" and value <= const:
                    matched_freq += freq

            # Si toutes les valeurs distinctes semblent couvertes par les MCV,
            # on peut retourner directement cette estimation.
            if column_stats.n_distinct > 0 and len(numeric_mcv_vals) >= int(column_stats.n_distinct):
                return max(0.0, min(matched_freq, 1.0))

            # Sinon, pour "=", on peut déjà s'en contenter si trouvé
            if op == "=" and matched_freq > 0:
                return max(0.0, min(matched_freq, 1.0))

            # Pour un range, si on a déjà une bonne part des valeurs dans MCV,
            # c'est une estimation utile.
            total_mcv_freq = sum(mcv_freqs)
            if total_mcv_freq >= 0.80:
                return max(0.0, min(matched_freq, 1.0 - null_frac))

    # ----------------------------------------------------------------
    # 2) Cas "=" avec n_distinct
    # ----------------------------------------------------------------
    if op == "=":
        if column_stats.n_distinct > 0:
            sel = 1.0 / max(column_stats.n_distinct, 1.0)
            return max(0.0, min(sel * (1.0 - null_frac), 1.0))
        return None

    # ----------------------------------------------------------------
    # 3) Fallback histogram_bounds pour les ranges
    # ----------------------------------------------------------------
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
# Safe recommendation engine
# --------------------------------------------------------------------

def evaluate_seq_scan_candidate(
    con,
    finding: ScanFinding,
    meta: TableMeta,
    query_stats: Optional[QueryStats] = None,
) -> Recommendation:
    if not finding.filter_expr:
        return Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason="Sequential scan without filter: an index would not be a safe recommendation.",
            filter_expr=finding.filter_expr,
        )

    if is_small_table(meta):
        return Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=(
                "Table is small; sequential scan is usually appropriate and "
                "an automatic index recommendation would not be safe."
            ),
            filter_expr=finding.filter_expr,
        )

    if finding.actual_total_time < 1.0 and not is_high_workload(query_stats):
        return Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason="Scan is already very fast and workload is not significant enough.",
            filter_expr=finding.filter_expr,
        )

    selected_fraction = estimate_selected_fraction(finding)
    if selected_fraction is None:
        return Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason="Unable to estimate filter selectivity safely from execution stats.",
            filter_expr=finding.filter_expr,
        )

    if selected_fraction >= 0.50:
        return Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=(
                f"Filter keeps a very large fraction of rows "
                f"({selected_fraction:.1%}); a sequential scan is likely appropriate."
            ),
            filter_expr=finding.filter_expr,
        )

    if selected_fraction >= 0.20:
        # on continue l'analyse, mais on ne classera pas ça en "safe"
        pass

    candidate_columns = extract_simple_filter_columns(
        finding.filter_expr,
        alias=finding.alias,
        table=finding.table,
    )

    if not candidate_columns:
        return Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason="Filter is not simple enough for a safe automatic index recommendation.",
            filter_expr=finding.filter_expr,
        )

    matched_index = find_equivalent_index(meta.indexes, candidate_columns)
    if matched_index:
        return Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason="An equivalent index already exists.",
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns,
            existing_index_match=matched_index,
        )

    if looks_like_prefix_search(finding.filter_expr):
        return Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="review",
            reason=(
                "Prefix LIKE filter detected. An index may help, but operator class / collation "
                "should be verified before recommending it automatically."
            ),
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns,
            create_index_sql=build_create_index_sql(finding.schema, finding.table, candidate_columns),
        )

    if looks_suspicious_predicate(finding.filter_expr):
        return Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="review",
            reason=(
                "Highly selective filter on a non-small table, but the predicate looks unusual. "
                "Review before creating an index."
            ),
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns,
            create_index_sql=build_create_index_sql(finding.schema, finding.table, candidate_columns),
        )

    # Validation par pg_stats pour réserver "safe" aux cas solides
    if len(candidate_columns) == 1:
        stats = load_column_stats(con, finding.schema, finding.table, candidate_columns[0])

        if stats is None:
            return Recommendation(
                schema=finding.schema,
                table=finding.table,
                confidence="review",
                reason=(
                    "Execution suggests a selective filtered scan, but pg_stats are unavailable; "
                    "review before creating the index."
                ),
                filter_expr=finding.filter_expr,
                candidate_columns=candidate_columns,
                create_index_sql=build_create_index_sql(finding.schema, finding.table, candidate_columns),
                stats_reason="No pg_stats entry found.",
            )

        estimated_selectivity = estimate_selectivity_from_stats(finding.filter_expr, stats)

        if estimated_selectivity is None:
            return Recommendation(
                schema=finding.schema,
                table=finding.table,
                confidence="review",
                reason=(
                    "Execution suggests a selective filtered scan, but column statistics do not "
                    "allow a confident selectivity estimate."
                ),
                filter_expr=finding.filter_expr,
                candidate_columns=candidate_columns,
                create_index_sql=build_create_index_sql(finding.schema, finding.table, candidate_columns),
                stats_reason="Could not estimate selectivity from pg_stats.",
            )

        if estimated_selectivity >= 0.20:
            return Recommendation(
                schema=finding.schema,
                table=finding.table,
                confidence="review",
                reason=(
                    "Execution was selective, but pg_stats suggest the predicate may not be "
                    "selective enough overall to justify a safe automatic index recommendation."
                ),
                filter_expr=finding.filter_expr,
                candidate_columns=candidate_columns,
                create_index_sql=build_create_index_sql(finding.schema, finding.table, candidate_columns),
                stats_reason=f"Estimated selectivity from pg_stats: {estimated_selectivity:.1%}",
            )

        return Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="safe",
            reason=(
                "Highly selective filtered sequential scan on a non-small table with no equivalent "
                "existing index, confirmed by pg_stats."
            ),
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns,
            create_index_sql=build_create_index_sql(finding.schema, finding.table, candidate_columns),
            stats_reason=f"Estimated selectivity from pg_stats: {estimated_selectivity:.1%}",
        )

    return Recommendation(
        schema=finding.schema,
        table=finding.table,
        confidence="review",
        reason="Multiple candidate columns detected. Review manually before creating a composite index.",
        filter_expr=finding.filter_expr,
        candidate_columns=candidate_columns,
        create_index_sql=build_create_index_sql(finding.schema, finding.table, candidate_columns),
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
    """
    Découpe uniquement sur les AND au niveau racine.
    """
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

        if depth == 0 and upper_expr[i:i+3] == "AND":
            prev_ok = (i == 0) or expr[i-1].isspace() or expr[i-1] == ")"
            next_ok = (i + 3 >= len(expr)) or expr[i+3].isspace() or expr[i+3] == "("

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
    """
    Cas supportés:
      alias.col = const
      alias.col > const
      alias.col >= const
      alias.col < const
      alias.col <= const
      alias.col LIKE 'abc%'
      alias.col ~~ 'abc%'
      AND entre clauses simples

    Tolère aussi:
      ((o.col)::text = 'x'::text)
      ((o.order_date >= '1998-04-21'::date) AND (o.order_date < '1998-04-17'::date))
    """
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
    """
    Vérifie un préfixe simple.
    Si candidate_columns == ['a'] et un index (a, b) existe, on considère que ça couvre.
    """
    wanted = [c.strip('"') for c in candidate_columns]

    for idx in indexes:
        cols = [c.strip('"') for c in idx.get("columns", [])]
        if cols[: len(wanted)] == wanted:
            return idx["index_name"]
    return None


def build_create_index_sql(schema: str, table: str, columns: List[str]) -> str:
    idx_name = f"idx_{table}_{'_'.join(columns)}"
    cols_sql = ", ".join(f'"{c}"' for c in columns)
    return f'CREATE INDEX "{idx_name}" ON "{schema}"."{table}" ({cols_sql});'


def is_high_workload(query_stats: Optional[QueryStats]) -> bool:
    if not query_stats:
        return False
    return (
        query_stats.calls >= 1000
        or query_stats.total_exec_time >= 5000
        or query_stats.mean_exec_time >= 5
    )


# --------------------------------------------------------------------
# Pretty printer / debug helper
# --------------------------------------------------------------------

def pretty_print_analysis(result: Dict[str, Any]) -> None:
    print(result["message"])
    for rec in result["recommendations"]:
        print("-" * 80)
        print(f"{rec['schema']}.{rec['table']}")
        print(f"confidence: {rec['confidence']}")
        print(f"reason: {rec['reason']}")
        if rec.get("filter_expr"):
            print(f"filter: {rec['filter_expr']}")
        if rec.get("candidate_columns"):
            print(f"columns: {rec['candidate_columns']}")
        if rec.get("existing_index_match"):
            print(f"existing_index: {rec['existing_index_match']}")
        if rec.get("stats_reason"):
            print(f"stats: {rec['stats_reason']}")
        if rec.get("create_index_sql"):
            print(f"sql: {rec['create_index_sql']}")