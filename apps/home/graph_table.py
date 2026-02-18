from __future__ import annotations

import re
import json
from typing import Dict, List, Tuple, Set, Optional, Any
from . import database

def generate_mermaid_table_dependencies_erdiagram(
    session: dict,
    table: str,
    max_depth: int = 5,
) -> Tuple[str, str]:
    """
    Generate a Mermaid ER diagram for:
      - a given base table
      - all tables it depends on via FOREIGN KEYS (outgoing FKs), recursively
    Includes:
      - all columns + data types
      - PK / FK markers
      - relationships with constraint names

    Returns: (mermaid_code, message). message is "" on success.
    """
    # ---- connect
    conn, msg = database.connectdb(session)
    if not conn:
        return "", msg or "Database connection failed."

    def _parse_table_name(t: str) -> Tuple[str, str]:
        t = (t or "").strip().strip('"')
        if "." in t:
            s, n = t.split(".", 1)
            return s.strip().strip('"'), n.strip().strip('"')
        return "public", t

    def _mermaid_entity_name(schema: str, name: str) -> str:
        # Mermaid identifiers: safest is uppercase + underscores
        raw = f"{schema}_{name}"
        raw = re.sub(r"[^0-9a-zA-Z_]+", "_", raw)
        raw = re.sub(r"_+", "_", raw).strip("_")
        return raw.upper()

    def _fetch_table_oid(cur, schema: str, name: str) -> Optional[int]:
        cur.execute(
            """
            SELECT c.oid
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
              AND c.relkind IN ('r','p')
            """,
            (schema, name),
        )
        row = cur.fetchone()
        return int(row[0]) if row else None

    def _fetch_columns(cur, schema: str, name: str) -> List[Tuple[str, str]]:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, name),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]

    def _fetch_pk_columns(cur, schema: str, name: str) -> Set[str]:
        cur.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = %s
              AND tc.table_name = %s
              AND tc.constraint_type = 'PRIMARY KEY'
            """,
            (schema, name),
        )
        return {r[0] for r in cur.fetchall()}

    def _fetch_outgoing_fks(cur, schema: str, name: str) -> List[Dict[str, Any]]:
        """
        Returns list of FK dicts:
          {
            fk_name,
            from_schema, from_table, from_cols[],
            to_schema, to_table, to_cols[]
          }
        """
        cur.execute(
            """
            WITH src AS (
              SELECT c.oid AS relid, n.nspname AS schema_name, c.relname AS table_name
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = %s
                AND c.relname = %s
                AND c.relkind IN ('r','p')
            )
            SELECT
              con.conname AS fk_name,
              nsrc.nspname AS from_schema,
              src.relname  AS from_table,
              ARRAY_AGG(src_att.attname ORDER BY k.ord) AS from_cols,
              ntgt.nspname AS to_schema,
              tgt.relname  AS to_table,
              ARRAY_AGG(tgt_att.attname ORDER BY k.ord) AS to_cols
            FROM pg_constraint con
            JOIN pg_class src       ON src.oid = con.conrelid
            JOIN pg_namespace nsrc  ON nsrc.oid = src.relnamespace
            JOIN pg_class tgt       ON tgt.oid = con.confrelid
            JOIN pg_namespace ntgt  ON ntgt.oid = tgt.relnamespace
            JOIN LATERAL (
              SELECT u.ord, u.src_attnum, v.tgt_attnum
              FROM unnest(con.conkey) WITH ORDINALITY u(src_attnum, ord)
              JOIN unnest(con.confkey) WITH ORDINALITY v(tgt_attnum, ord)
                USING (ord)
            ) k ON true
            JOIN pg_attribute src_att ON src_att.attrelid = src.oid AND src_att.attnum = k.src_attnum
            JOIN pg_attribute tgt_att ON tgt_att.attrelid = tgt.oid AND tgt_att.attnum = k.tgt_attnum
            WHERE con.contype = 'f'
              AND con.conrelid IN (SELECT relid FROM src)
            GROUP BY con.conname, nsrc.nspname, src.relname, ntgt.nspname, tgt.relname
            ORDER BY ntgt.nspname, tgt.relname, con.conname
            """,
            (schema, name),
        )

        out = []
        for fk_name, fs, ft, fcols, ts, tt, tcols in cur.fetchall():
            out.append(
                {
                    "fk_name": fk_name,
                    "from_schema": fs,
                    "from_table": ft,
                    "from_cols": list(fcols or []),
                    "to_schema": ts,
                    "to_table": tt,
                    "to_cols": list(tcols or []),
                }
            )
        return out

    # ---- crawl dependencies
    base_schema, base_table = _parse_table_name(table)

    tables_seen: Set[Tuple[str, str]] = set()
    tables_order: List[Tuple[str, str]] = []
    fks_all: List[Dict[str, Any]] = []

    queue: List[Tuple[str, str, int]] = [(base_schema, base_table, 0)]

    try:
        with conn.cursor() as cur:
            while queue:
                sch, tbl, depth = queue.pop(0)
                key = (sch, tbl)
                if key in tables_seen:
                    continue
                tables_seen.add(key)
                tables_order.append(key)

                # get outgoing fks and enqueue referenced tables (dependencies)
                fks = _fetch_outgoing_fks(cur, sch, tbl)
                fks_all.extend(fks)

                if depth < max_depth:
                    for fk in fks:
                        dep = (fk["to_schema"], fk["to_table"])
                        if dep not in tables_seen:
                            queue.append((dep[0], dep[1], depth + 1))

            # gather columns/pk/fk-columns for all included tables
            cols_map: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
            pk_map: Dict[Tuple[str, str], Set[str]] = {}
            fkcols_map: Dict[Tuple[str, str], Set[str]] = {k: set() for k in tables_seen}

            for fk in fks_all:
                from_key = (fk["from_schema"], fk["from_table"])
                if from_key in fkcols_map:
                    fkcols_map[from_key].update(fk["from_cols"])

            for sch, tbl in tables_order:
                cols_map[(sch, tbl)] = _fetch_columns(cur, sch, tbl)
                pk_map[(sch, tbl)] = _fetch_pk_columns(cur, sch, tbl)

    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        return "", f"Error while generating Mermaid diagram: {e}"

    finally:
        try:
            conn.close()
        except Exception:
            pass

    # ---- build mermaid erDiagram
    lines: List[str] = ["erDiagram"]

    # entity blocks
    for sch, tbl in tables_order:
        ent = _mermaid_entity_name(sch, tbl)
        lines.append(f"    {ent} {{")
        pk_cols = pk_map.get((sch, tbl), set())
        fk_cols = fkcols_map.get((sch, tbl), set())
        for col_name, data_type in cols_map.get((sch, tbl), []):
            # Mermaid erDiagram expects: "<datatype> <attribute> [PK] [FK]"
            flags = []
            if col_name in pk_cols:
                flags.append("PK")
            if col_name in fk_cols:
                flags.append("FK")
            flags_str = (" " + ", ".join(flags)) if flags else ""
            # data_type can contain spaces (e.g. "character varying") -> Mermaid generally accepts it,
            # but safest is to replace spaces with underscore.
            dtype = re.sub(r"\s+", "_", (data_type or "text").strip())
            lines.append(f"        {dtype} {col_name}{flags_str}")
        lines.append("    }")
        lines.append("")

    # relationships (only those within included set)
    included = set(tables_seen)
    for fk in fks_all:
        a = (fk["from_schema"], fk["from_table"])
        b = (fk["to_schema"], fk["to_table"])
        if a not in included or b not in included:
            continue
        from_ent = _mermaid_entity_name(*a)
        to_ent = _mermaid_entity_name(*b)
        # convention: referenced table is "one", referencing table is "many"
        # so: TO ||--o{ FROM
        lines.append(f"    {to_ent} ||--o{{ {from_ent} : {fk['fk_name']}")

    return "\n".join(lines), ""