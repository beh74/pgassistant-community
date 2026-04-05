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
            resolved_tables = _resolve_input_relations(cur, tables_pairs)

            pk_cols = _fetch_pk_columns(cur, resolved_tables)
            fk_edges = _fetch_fk_edges(cur, resolved_tables)
            est_rows = _fetch_est_rows(cur, resolved_tables) if include_est_rows else {}

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
def _resolve_input_relations(cur, tables: List[Tuple[str, str]]) -> List[Dict[str, Any]]:
    """
    Resolve exact input (schema, table) pairs to rel OIDs once, then reuse those OIDs
    for PK/FK/stats lookups.

    Returns a list of dicts:
      {
        "schema": ...,
        "table": ...,
        "full_name": "schema.table",
        "oid": ...,
        "relkind": ...
      }
    """
    if not tables:
        return []

    values_sql = ",".join(["(%s,%s)"] * len(tables))
    params = [x for pair in tables for x in pair]

    sql = f"""
    WITH input_tables(schema_name, table_name) AS (
      VALUES {values_sql}
    )
    SELECT
      it.schema_name,
      it.table_name,
      c.oid,
      c.relkind
    FROM input_tables it
    JOIN pg_namespace n
      ON n.nspname = it.schema_name
    JOIN pg_class c
      ON c.relnamespace = n.oid
     AND c.relname = it.table_name
    WHERE c.relkind IN ('r', 'p', 'm')
    ORDER BY it.schema_name, it.table_name;
    """

    cur.execute(sql, params)

    out = []
    for schema_name, table_name, oid, relkind in cur.fetchall():
        out.append(
            {
                "schema": schema_name,
                "table": table_name,
                "full_name": f"{schema_name}.{table_name}",
                "oid": oid,
                "relkind": relkind,
            }
        )
    return out


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


def _fetch_pk_columns(cur, resolved_tables: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    if not resolved_tables:
        return {}

    relids = [r["oid"] for r in resolved_tables]
    full_name_by_oid = {r["oid"]: r["full_name"] for r in resolved_tables}

    sql = """
    SELECT
      con.conrelid,
      a.attname AS col_name,
      u.ord     AS ord
    FROM pg_constraint con
    JOIN LATERAL unnest(con.conkey) WITH ORDINALITY u(attnum, ord) ON true
    JOIN pg_attribute a
      ON a.attrelid = con.conrelid
     AND a.attnum = u.attnum
    WHERE con.contype = 'p'
      AND con.conrelid = ANY(%s)
    ORDER BY con.conrelid, u.ord;
    """

    cur.execute(sql, (relids,))

    out = defaultdict(list)
    for conrelid, col_name, _ord in cur.fetchall():
        full_name = full_name_by_oid.get(conrelid)
        if full_name:
            out[full_name].append(col_name)

    return dict(out)


def _fetch_fk_edges(cur, resolved_tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not resolved_tables:
        return []

    relids = [r["oid"] for r in resolved_tables]
    full_name_by_oid = {r["oid"]: r["full_name"] for r in resolved_tables}

    sql = """
    SELECT
      con.conrelid,
      con.confrelid,
      con.conname AS fk_name,
      ARRAY_AGG(src_att.attname ORDER BY k.ord) AS from_cols,
      ARRAY_AGG(tgt_att.attname ORDER BY k.ord) AS to_cols
    FROM pg_constraint con
    JOIN LATERAL (
      SELECT u.ord, u.src_attnum, v.tgt_attnum
      FROM unnest(con.conkey)  WITH ORDINALITY u(src_attnum, ord)
      JOIN unnest(con.confkey) WITH ORDINALITY v(tgt_attnum, ord) USING (ord)
    ) k ON true
    JOIN pg_attribute src_att
      ON src_att.attrelid = con.conrelid
     AND src_att.attnum = k.src_attnum
    JOIN pg_attribute tgt_att
      ON tgt_att.attrelid = con.confrelid
     AND tgt_att.attnum = k.tgt_attnum
    WHERE con.contype = 'f'
      AND con.conrelid = ANY(%s)
      AND con.confrelid = ANY(%s)
    GROUP BY con.conrelid, con.confrelid, con.conname
    ORDER BY con.conrelid, con.conname;
    """

    cur.execute(sql, (relids, relids))

    edges = []
    for conrelid, confrelid, fk_name, from_cols, to_cols in cur.fetchall():
        from_full = full_name_by_oid.get(conrelid)
        to_full = full_name_by_oid.get(confrelid)
        if not from_full or not to_full:
            continue

        edges.append(
            {
                "from_table": from_full,
                "to_table": to_full,
                "fk_name": fk_name,
                "from_cols": list(from_cols),
                "to_cols": list(to_cols),
            }
        )

    return edges


def _fetch_est_rows(cur, resolved_tables: List[Dict[str, Any]]) -> Dict[str, float]:
    if not resolved_tables:
        return {}

    out = {}
    for r in resolved_tables:
        out[r["full_name"]] = 0.0

    sql = """
    SELECT oid, reltuples
    FROM pg_class
    WHERE oid = ANY(%s);
    """

    relids = [r["oid"] for r in resolved_tables]
    full_name_by_oid = {r["oid"]: r["full_name"] for r in resolved_tables}

    cur.execute(sql, (relids,))

    for oid, reltuples in cur.fetchall():
        full_name = full_name_by_oid.get(oid)
        if full_name:
            out[full_name] = float(reltuples or 0.0)

    return out