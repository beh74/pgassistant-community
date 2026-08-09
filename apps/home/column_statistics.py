"""Readable pg_stats data for the Table Advisor."""


def _parse_array(value):
    if value is None or not value.startswith("{") or not value.endswith("}"):
        return []
    result, current, quoted, escaped = [], [], False, False
    for character in value[1:-1]:
        if escaped:
            current.append(character)
            escaped = False
        elif character == "\\":
            escaped = True
        elif character == '"':
            quoted = not quoted
        elif character == "," and not quoted:
            result.append("".join(current))
            current = []
        else:
            current.append(character)
    if current or value != "{}":
        result.append("".join(current))
    return result


def _parse_float_array(value):
    try:
        return [float(item) for item in _parse_array(value)]
    except (TypeError, ValueError):
        return []


COLUMN_STATS_SQL = """
SELECT
    s.attname,
    s.null_frac,
    s.avg_width,
    s.n_distinct,
    s.most_common_vals::text,
    s.most_common_freqs::text,
    s.histogram_bounds::text,
    s.correlation,
    COALESCE(c.reltuples, 0) AS estimated_rows,
    st.last_analyze,
    st.last_autoanalyze
FROM pg_catalog.pg_stats AS s
JOIN pg_catalog.pg_namespace AS n ON n.nspname = s.schemaname
JOIN pg_catalog.pg_class AS c ON c.relnamespace = n.oid AND c.relname = s.tablename
LEFT JOIN pg_catalog.pg_stat_all_tables AS st
  ON st.schemaname = s.schemaname AND st.relname = s.tablename
WHERE s.schemaname = %s AND s.tablename = %s
ORDER BY s.attname
"""


def _iso(value):
    return value.isoformat() if value is not None else None


def load_column_statistics(conn, schema_name, table_name):
    with conn.cursor() as cursor:
        cursor.execute(COLUMN_STATS_SQL, (schema_name, table_name))
        rows = cursor.fetchall()

    columns = []
    for row in rows:
        (
            column_name, null_frac, avg_width, n_distinct, common_values_text,
            common_freqs_text, histogram_text, correlation, estimated_rows,
            last_analyze, last_autoanalyze,
        ) = row
        common_values = _parse_array(common_values_text)
        common_freqs = _parse_float_array(common_freqs_text)
        histogram = _parse_array(histogram_text)
        n_distinct = float(n_distinct or 0)
        estimated_rows = max(float(estimated_rows or 0), 0)
        distinct_estimate = (
            round(abs(n_distinct) * estimated_rows)
            if n_distinct < 0
            else round(n_distinct)
        )
        most_common = [
            {
                "value": str(value),
                "frequency_pct": round((common_freqs[index] if index < len(common_freqs) else 0) * 100, 2),
            }
            for index, value in enumerate(common_values[:10])
        ]
        columns.append({
            "column": column_name,
            "null_fraction": float(null_frac or 0),
            "null_percent": round(float(null_frac or 0) * 100, 2),
            "average_width_bytes": int(avg_width or 0),
            "n_distinct": n_distinct,
            "distinct_estimate": distinct_estimate,
            "correlation": float(correlation) if correlation is not None else None,
            "most_common_values": most_common,
            "histogram_bounds_count": len(histogram),
        })

    estimated_table_rows = max(float(rows[0][8] or 0), 0) if rows else 0
    return {
        "success": True,
        "table": f"{schema_name}.{table_name}",
        "estimated_rows": round(estimated_table_rows),
        "last_analyze": _iso(rows[0][9]) if rows else None,
        "last_autoanalyze": _iso(rows[0][10]) if rows else None,
        "columns": columns,
    }
