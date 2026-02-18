from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Tuple
from . import database

def build_mermaid_erd_from_explain_stats(
    explain_stats: Dict[str, Any],
    session,
    *,
    include_est_rows: bool = False,
) -> Tuple[str, str]:
    """
    Build a Mermaid erDiagram from decode_explain_json_with_buffers() output.

    - explain_stats: dict returned by decode_explain_json_with_buffers()
    - session: Flask session (used by database.connectdb(session))
    - database: your module/object that provides connectdb(session) -> (conn, message)
    - include_est_rows: if True, append an est_rows pseudo-field (optional)

    Returns: (mermaid_code, message)
      - mermaid_code: Mermaid ER diagram as string (empty if error)
      - message: status / error message (empty if OK)
    """

    # ---- 1) Validate and aggregate by table ----
    by_table = explain_stats.get("by_table") or []
    if not isinstance(by_table, list):
        return "", "Invalid explain_stats: 'by_table' must be a list."

    agg = defaultdict(lambda: {
        "self_time_pct": 0.0,
        "self_time_ms": 0.0,
        "top_node": None,
        "top_node_ms": 0.0,
    })

    for r in by_table:
        t = r.get("table")
        if not t:
            continue
        ms = float(r.get("self_time_ms") or 0.0)
        pct = float(r.get("self_time_pct") or 0.0)
        node_type = r.get("node_type") or "Unknown"

        a = agg[t]
        a["self_time_ms"] += ms
        a["self_time_pct"] += pct
        if ms > a["top_node_ms"]:
            a["top_node_ms"] = ms
            a["top_node"] = node_type

    tables_full = sorted(agg.keys())
    if not tables_full:
        return "erDiagram\n", ""  # nothing to draw

    tables_pairs: List[Tuple[str, str]] = []
    for t in tables_full:
        if "." in t:
            schema, name = t.split(".", 1)
        else:
            schema, name = "public", t
        tables_pairs.append((schema, name))

    # ---- 2) Connect DB ----
    conn, msg = database.connectdb(session)
    if not conn:
        return "", msg or "Database connection failed."

    try:
        with conn.cursor() as cur:
            pk_cols = _fetch_pk_columns(cur, tables_pairs)
            fk_edges = _fetch_fk_edges(cur, tables_pairs)
            est_rows = _fetch_est_rows(cur, tables_pairs) if include_est_rows else {}

        # Child-side FK columns for box content
        fk_cols_by_table = defaultdict(list)
        for e in fk_edges:
            child = e["from_table"]
            for c in e["from_cols"]:
                if c not in fk_cols_by_table[child]:
                    fk_cols_by_table[child].append(c)

        # ---- 3) Build Mermaid ER diagram ----
        lines: List[str] = ["erDiagram"]

        # Entities
        for table in tables_full:
            ent = _mermaid_entity_id(table)

            pks = pk_cols.get(table, [])
            fks = fk_cols_by_table.get(table, [])

            fk_only = [c for c in fks if c not in pks]

            lines.append(f"    {ent} {{")
            for c in pks:
                flag = "PK"
                if c in fks:
                    flag = "PK, FK"
                lines.append(f"        _ {c} {flag}")
            for c in fk_only:
                lines.append(f"        _ {c} FK")

            # Optional: show estimate as pseudo-field
            if include_est_rows:
                lines.append(f"        _ est_rows ~{int(est_rows.get(table, 0))}~")

            lines.append("    }")
            lines.append("")

        # Relationships (Parent -> Child)
        for e in fk_edges:
            parent = _mermaid_entity_id(e["to_table"])
            child = _mermaid_entity_id(e["from_table"])
            lines.append(f"    {parent} ||--o{{ {child} : {e['fk_name']}")

        lines.append("")

        # Styles (no semicolons)
        lines += [
            "    classDef load0 fill:#f3f7fb,stroke:#336791,stroke-width:1px,color:#0b2239",
            "    classDef load1 fill:#dbe9f6,stroke:#336791,stroke-width:1px,color:#0b2239",
            "    classDef load2 fill:#b9d2ee,stroke:#336791,stroke-width:2px,color:#0b2239",
            "    classDef load3 fill:#7fb0df,stroke:#336791,stroke-width:3px,color:#061a2b",
            # keep not-too-dark to avoid ERD text disappearing
            "    classDef load4 fill:#5b97cf,stroke:#1f3f5c,stroke-width:5px,color:#061a2b",
            "",
        ]

        for table in tables_full:
            ent = _mermaid_entity_id(table)
            bucket = _pct_to_bucket(float(agg[table]["self_time_pct"]))
            lines.append(f"    class {ent} {bucket}")

        return "\n".join(lines), ""

    except Exception as e:
        return "", f"Mermaid ERD generation failed: {e}"
    finally:
        # Depending on your app, you might not want to close here.
        # If connectdb returns a short-lived connection, close it.
        try:
            conn.close()
        except Exception:
            pass


# ---------------- internal helpers ----------------

def _mermaid_entity_id(schema_table: str) -> str:
    return schema_table.replace(".", "_").replace("-", "_").upper()


def _pct_to_bucket(pct: float) -> str:
    if pct <= 5:
        return "load0"
    if pct <= 15:
        return "load1"
    if pct <= 30:
        return "load2"
    if pct <= 50:
        return "load3"
    return "load4"


def _fetch_pk_columns(cur, tables: List[Tuple[str, str]]) -> Dict[str, List[str]]:
    if not tables:
        return {}

    values_sql = ",".join(["(%s,%s)"] * len(tables))
    params = [x for pair in tables for x in pair]

    sql = f"""
    WITH input_tables(schema_name, table_name) AS (
      VALUES {values_sql}
    ),
    tbl AS (
      SELECT c.oid AS relid
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN input_tables it ON it.schema_name = n.nspname AND it.table_name = c.relname
      WHERE c.relkind IN ('r','p')
    )
    SELECT
      n.nspname AS schema_name,
      c.relname AS table_name,
      a.attname AS col_name,
      u.ord     AS ord
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN tbl t ON t.relid = c.oid
    JOIN LATERAL unnest(con.conkey) WITH ORDINALITY u(attnum, ord) ON true
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = u.attnum
    WHERE con.contype = 'p'
    ORDER BY schema_name, table_name, ord;
    """
    cur.execute(sql, params)

    out = defaultdict(list)
    for schema_name, table_name, col_name, _ord in cur.fetchall():
        out[f"{schema_name}.{table_name}"].append(col_name)
    return dict(out)


def _fetch_fk_edges(cur, tables: List[Tuple[str, str]]) -> List[Dict[str, Any]]:
    if not tables:
        return []

    values_sql = ",".join(["(%s,%s)"] * len(tables))
    params = [x for pair in tables for x in pair]

    sql = f"""
    WITH input_tables(schema_name, table_name) AS (
      VALUES {values_sql}
    ),
    tbl AS (
      SELECT c.oid AS relid
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN input_tables it ON it.schema_name = n.nspname AND it.table_name = c.relname
      WHERE c.relkind IN ('r','p')
    )
    SELECT
      nsrc.nspname AS from_schema,
      src.relname  AS from_table,
      con.conname  AS fk_name,
      ARRAY_AGG(src_att.attname ORDER BY k.ord) AS from_cols,
      ntgt.nspname AS to_schema,
      tgt.relname  AS to_table,
      ARRAY_AGG(tgt_att.attname ORDER BY k.ord) AS to_cols
    FROM pg_constraint con
    JOIN pg_class src      ON src.oid = con.conrelid
    JOIN pg_namespace nsrc ON nsrc.oid = src.relnamespace
    JOIN pg_class tgt      ON tgt.oid = con.confrelid
    JOIN pg_namespace ntgt ON ntgt.oid = tgt.relnamespace
    JOIN LATERAL (
      SELECT u.ord, u.src_attnum, v.tgt_attnum
      FROM unnest(con.conkey) WITH ORDINALITY u(src_attnum, ord)
      JOIN unnest(con.confkey) WITH ORDINALITY v(tgt_attnum, ord) USING (ord)
    ) k ON true
    JOIN pg_attribute src_att ON src_att.attrelid = src.oid AND src_att.attnum = k.src_attnum
    JOIN pg_attribute tgt_att ON tgt_att.attrelid = tgt.oid AND tgt_att.attnum = k.tgt_attnum
    WHERE con.contype='f'
      AND con.conrelid  IN (SELECT relid FROM tbl)
      AND con.confrelid IN (SELECT relid FROM tbl)
    GROUP BY
      nsrc.nspname, src.relname, con.conname,
      ntgt.nspname, tgt.relname
    ORDER BY 1,2,3;
    """
    cur.execute(sql, params)

    edges = []
    for from_schema, from_table, fk_name, from_cols, to_schema, to_table, to_cols in cur.fetchall():
        edges.append({
            "from_table": f"{from_schema}.{from_table}",
            "to_table": f"{to_schema}.{to_table}",
            "fk_name": fk_name,
            "from_cols": list(from_cols),
            "to_cols": list(to_cols),
        })
    return edges


def _fetch_est_rows(cur, tables: List[Tuple[str, str]]) -> Dict[str, float]:
    if not tables:
        return {}

    values_sql = ",".join(["(%s,%s)"] * len(tables))
    params = [x for pair in tables for x in pair]

    sql = f"""
    WITH input_tables(schema_name, table_name) AS (
      VALUES {values_sql}
    )
    SELECT n.nspname, c.relname, c.reltuples
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN input_tables it ON it.schema_name = n.nspname AND it.table_name = c.relname
    WHERE c.relkind IN ('r','p');
    """
    cur.execute(sql, params)

    out = {}
    for schema_name, table_name, reltuples in cur.fetchall():
        out[f"{schema_name}.{table_name}"] = float(reltuples or 0.0)
    return out