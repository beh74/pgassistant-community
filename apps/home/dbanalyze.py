from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict


# -----------------------------
# Data model
# -----------------------------

@dataclass
class BufferMetrics:
    shared_hit: int = 0
    shared_read: int = 0
    shared_dirtied: int = 0
    shared_written: int = 0
    local_hit: int = 0
    local_read: int = 0
    local_dirtied: int = 0
    local_written: int = 0
    temp_read: int = 0
    temp_written: int = 0

    def add(self, other: "BufferMetrics") -> None:
        for f in self.__dataclass_fields__:
            setattr(self, f, getattr(self, f) + getattr(other, f))

    def to_dict(self) -> Dict[str, int]:
        return {f: getattr(self, f) for f in self.__dataclass_fields__}


@dataclass
class NodeMetrics:
    node_type: str
    inclusive_ms: float
    self_ms: float
    self_rows: float
    relation: Optional[str] = None
    schema: Optional[str] = None
    index_name: Optional[str] = None
    buffers: BufferMetrics = field(default_factory=BufferMetrics)


# -----------------------------
# Helpers
# -----------------------------

def _get(d: Dict[str, Any], key: str, default: Any = None) -> Any:
    return d.get(key, default)


def _float(node: Dict[str, Any], key: str, default: float = 0.0) -> float:
    v = _get(node, key, default)
    try:
        return float(v)
    except (TypeError, ValueError):
        return float(default)


def _int(d: Dict[str, Any], key: str) -> int:
    v = d.get(key, 0)
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _node_inclusive_ms(node: Dict[str, Any]) -> float:
    # In EXPLAIN JSON, "Actual Total Time" is in milliseconds.
    # Multiply by loops to approximate total time across loops.
    total = _float(node, "Actual Total Time", 0.0)
    loops = _float(node, "Actual Loops", 1.0) or 1.0
    return total * loops


def _node_self_rows(node: Dict[str, Any]) -> float:
    rows = _float(node, "Actual Rows", 0.0)
    loops = _float(node, "Actual Loops", 1.0) or 1.0
    return rows * loops


def _parse_buffers(node: Dict[str, Any]) -> BufferMetrics:
    """
    For BUFFERS, JSON typically contains keys like:
      "Shared Hit Blocks", "Shared Read Blocks", "Shared Dirtied Blocks", "Shared Written Blocks"
      "Local Hit Blocks",  ...
      "Temp Read Blocks", "Temp Written Blocks"
    """
    bm = BufferMetrics()

    bm.shared_hit = _int(node, "Shared Hit Blocks")
    bm.shared_read = _int(node, "Shared Read Blocks")
    bm.shared_dirtied = _int(node, "Shared Dirtied Blocks")
    bm.shared_written = _int(node, "Shared Written Blocks")

    bm.local_hit = _int(node, "Local Hit Blocks")
    bm.local_read = _int(node, "Local Read Blocks")
    bm.local_dirtied = _int(node, "Local Dirtied Blocks")
    bm.local_written = _int(node, "Local Written Blocks")

    bm.temp_read = _int(node, "Temp Read Blocks")
    bm.temp_written = _int(node, "Temp Written Blocks")

    return bm


# -----------------------------
# Plan walk (time exclusive)
# -----------------------------

def _walk_plan_collect(node: Dict[str, Any], out: List[NodeMetrics]) -> float:
    """
    Walk the plan tree and compute inclusive and exclusive (self) time.
    Returns inclusive_ms for this node.
    """
    node_type = str(_get(node, "Node Type", "UNKNOWN"))
    inclusive_ms = _node_inclusive_ms(node)
    self_rows = _node_self_rows(node)

    # recurse children
    child_inclusive_sum = 0.0
    for child in _get(node, "Plans", []) or []:
        child_inclusive_sum += _walk_plan_collect(child, out)

    self_ms = inclusive_ms - child_inclusive_sum
    if self_ms < 0:
        # rounding / instrumentation artifacts can cause tiny negatives
        self_ms = 0.0

    schema = _get(node, "Schema")
    relation = _get(node, "Relation Name")
    index_name = _get(node, "Index Name")

    buffers = _parse_buffers(node)

    out.append(
        NodeMetrics(
            node_type=node_type,
            inclusive_ms=inclusive_ms,
            self_ms=self_ms,
            self_rows=self_rows,
            relation=relation,
            schema=schema,
            index_name=index_name,
            buffers=buffers,
        )
    )
    return inclusive_ms


# -----------------------------
# Aggregation
# -----------------------------

def _agg_row_init() -> Dict[str, Any]:
    return {
        "count": 0,
        "self_time_ms": 0.0,
        "self_rows": 0.0,
        "buffers": BufferMetrics(),
    }


def _agg_add(agg: Dict[str, Any], n: NodeMetrics) -> None:
    agg["count"] += 1
    agg["self_time_ms"] += n.self_ms
    agg["self_rows"] += n.self_rows
    agg["buffers"].add(n.buffers)


def _finalize_rows(rows: List[Dict[str, Any]], total_ms: float) -> List[Dict[str, Any]]:
    total_ms = total_ms if total_ms > 0 else 1.0
    for r in rows:
        r["self_time_pct"] = 100.0 * r["self_time_ms"] / total_ms
        # flatten buffers
        buf: BufferMetrics = r.pop("buffers")
        r.update(buf.to_dict())
    rows.sort(key=lambda x: x["self_time_ms"], reverse=True)
    return rows


def decode_explain_json_with_buffers(
    explain_json: str | List[Dict[str, Any]] | Dict[str, Any],
    include_top_nodes: bool = True,
    top_n: int = 25,
) -> Dict[str, Any]:
    """
    Parse EXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON) output and aggregate:
      - by_node_type
      - by_table
      - by_index

    Also classifies a dominant factor:
      - planner_dominated
      - execution_dominated
      - io_dominated
      - cpu_dominated
    """
    if isinstance(explain_json, str):
        doc = json.loads(explain_json)
    else:
        doc = explain_json

    roots = [doc] if isinstance(doc, dict) else doc
    if not roots or not isinstance(roots[0], dict):
        raise ValueError("Unexpected JSON structure for EXPLAIN (FORMAT JSON).")

    root = roots[0]
    plan = root.get("Plan")
    if not isinstance(plan, dict):
        raise ValueError("Missing 'Plan' in EXPLAIN JSON.")

    execution_time_ms = float(root.get("Execution Time", 0.0) or 0.0)
    planning_time_ms = float(root.get("Planning Time", 0.0) or 0.0)

    nodes: List[NodeMetrics] = []
    _walk_plan_collect(plan, nodes)

    # Use Execution Time as denominator; if missing, fall back to sum of self times
    denom_ms = execution_time_ms if execution_time_ms > 0 else sum(n.self_ms for n in nodes)
    if denom_ms <= 0:
        denom_ms = 1.0

    # -------------------- Dominant factor classification --------------------
    # Aggregate buffers across nodes
    buf_sum = {
        "shared_hit": 0, "shared_read": 0, "shared_dirtied": 0, "shared_written": 0,
        "local_hit": 0, "local_read": 0, "local_dirtied": 0, "local_written": 0,
        "temp_read": 0, "temp_written": 0,
    }
    for n in nodes:
        b = n.buffers
        buf_sum["shared_hit"] += int(getattr(b, "shared_hit", 0) or 0)
        buf_sum["shared_read"] += int(getattr(b, "shared_read", 0) or 0)
        buf_sum["shared_dirtied"] += int(getattr(b, "shared_dirtied", 0) or 0)
        buf_sum["shared_written"] += int(getattr(b, "shared_written", 0) or 0)

        buf_sum["local_hit"] += int(getattr(b, "local_hit", 0) or 0)
        buf_sum["local_read"] += int(getattr(b, "local_read", 0) or 0)
        buf_sum["local_dirtied"] += int(getattr(b, "local_dirtied", 0) or 0)
        buf_sum["local_written"] += int(getattr(b, "local_written", 0) or 0)

        buf_sum["temp_read"] += int(getattr(b, "temp_read", 0) or 0)
        buf_sum["temp_written"] += int(getattr(b, "temp_written", 0) or 0)

    total_time_ms = planning_time_ms + execution_time_ms
    planning_ratio = (planning_time_ms / total_time_ms) if total_time_ms > 0 else 0.0
    execution_ratio = (execution_time_ms / total_time_ms) if total_time_ms > 0 else 0.0

    read_blocks = buf_sum["shared_read"] + buf_sum["local_read"] + buf_sum["temp_read"]
    hit_blocks = buf_sum["shared_hit"] + buf_sum["local_hit"]
    temp_ops = buf_sum["temp_read"] + buf_sum["temp_written"]

    total_buf_ops = (
        read_blocks
        + hit_blocks
        + buf_sum["temp_written"]
        + buf_sum["shared_written"]
        + buf_sum["local_written"]
    )
    read_ratio = (read_blocks / total_buf_ops) if total_buf_ops > 0 else 0.0

    # Thresholds (tweakable)
    PLANNING_ABS_MS = 1.0
    PLANNING_DOM_RATIO = 0.60

    IO_READ_RATIO = 0.20
    IO_READ_BLOCKS = 256
    IO_TEMP_OPS = 128

    CPU_LOW_READ_RATIO = 0.05
    CPU_MIN_EXEC_MS = 2.0

    # Scores
    score_planner = 0.0
    if planning_time_ms >= PLANNING_ABS_MS:
        score_planner = planning_ratio  # 0..1

    score_io = 0.0
    if read_ratio >= IO_READ_RATIO:
        score_io += min(1.0, read_ratio / 0.50)
    if read_blocks >= IO_READ_BLOCKS:
        score_io += 0.3
    if temp_ops >= IO_TEMP_OPS:
        score_io += 0.4
    score_io = min(1.5, score_io)

    score_cpu = 0.0
    if execution_time_ms >= CPU_MIN_EXEC_MS and execution_ratio >= 0.60 and read_ratio <= CPU_LOW_READ_RATIO and temp_ops == 0:
        score_cpu = 0.9
    elif execution_time_ms >= CPU_MIN_EXEC_MS and execution_ratio >= 0.60 and read_ratio <= CPU_LOW_READ_RATIO:
        score_cpu = 0.6

    score_exec = execution_ratio

    scores = {
        "planner_dominated": score_planner,
        "io_dominated": score_io,
        "cpu_dominated": score_cpu,
        "execution_dominated": score_exec,
    }

    dominant_factor = max(scores, key=scores.get) if scores else "execution_dominated"

    # Guardrails / tie-breaks
    low_confidence = False

    # 1) Strong planner domination (normal-sized timings)
    if planning_time_ms >= PLANNING_ABS_MS and planning_ratio >= PLANNING_DOM_RATIO:
        dominant_factor = "planner_dominated"
    else:
        # 2) IO/CPU overrides when execution dominates (normal case)
        if score_io >= 0.8 and execution_ratio >= 0.5:
            dominant_factor = "io_dominated"
        elif score_cpu >= 0.8 and execution_ratio >= 0.5:
            dominant_factor = "cpu_dominated"

    # 3) Tiny-query override: avoid misleading fallbacks on sub-ms totals
    TINY_TOTAL_MS = 1.0
    TINY_RATIO_DOM = 0.55  # slightly softer than PLANNING_DOM_RATIO

    if total_time_ms > 0 and total_time_ms < TINY_TOTAL_MS:
        low_confidence = True
        # For tiny timings, decide by ratio (and direction), even if PLANNING_ABS_MS blocks it
        if planning_time_ms > execution_time_ms and planning_ratio >= TINY_RATIO_DOM:
            dominant_factor = "planner_dominated"
        elif execution_time_ms >= planning_time_ms and execution_ratio >= TINY_RATIO_DOM:
            dominant_factor = "execution_dominated"
        else:
            # keep whatever was chosen above, but it's still low confidence
            pass

    if dominant_factor == "planner_dominated":
        dominant_explain = (
            "Planning time dominates execution. If this query runs frequently, consider prepared statements / plan caching."
        )
        if low_confidence:
            dominant_explain = (
                "Planning exceeds execution, but timings are sub-millisecond (low confidence). "
                "This often reflects measurement noise and planner overhead on trivial queries."
            )
    elif dominant_factor == "io_dominated":
        dominant_explain = (
            "Buffer reads (and/or temp activity) are significant, suggesting IO-bound execution. "
            "Consider indexes, reducing scanned rows, work_mem (if temp spill), and cache effectiveness."
        )
    elif dominant_factor == "cpu_dominated":
        dominant_explain = (
            "Execution time dominates while buffer reads are low (mostly cache hits), suggesting CPU-bound work "
            "(joins/aggregates/sorts/functions). Consider reducing row counts earlier, optimizing joins/expressions, "
            "and checking for expensive functions."
        )
    else:
        dominant_explain = (
            "Execution time dominates overall. Investigate the most expensive nodes (top_nodes) and table/index breakdown."
        )
        if low_confidence:
            dominant_explain = (
                "Execution exceeds planning, but timings are sub-millisecond (low confidence). "
                "This often reflects measurement noise on trivial queries."
            )
    # -----------------------------------------------------------------------

    # 1) by node type
    by_node_type = defaultdict(_agg_row_init)
    for n in nodes:
        _agg_add(by_node_type[n.node_type], n)

    node_type_rows = []
    for node_type, agg in by_node_type.items():
        node_type_rows.append({
            "node_type": node_type,
            "count": agg["count"],
            "self_time_ms": agg["self_time_ms"],
            "self_rows": agg["self_rows"],
            "buffers": agg["buffers"],
        })
    node_type_rows = _finalize_rows(node_type_rows, denom_ms)

    # 2) by table and node type
    by_table = defaultdict(_agg_row_init)
    for n in nodes:
        if n.relation:
            table = f"{n.schema}.{n.relation}" if n.schema else n.relation
            key = (table, n.node_type)
            _agg_add(by_table[key], n)

    table_rows = []
    for (table, node_type), agg in by_table.items():
        table_rows.append({
            "table": table,
            "node_type": node_type,
            "count": agg["count"],
            "self_time_ms": agg["self_time_ms"],
            "self_rows": agg["self_rows"],
            "buffers": agg["buffers"],
        })
    table_rows = _finalize_rows(table_rows, denom_ms)

    # 3) by index and node type
    by_index = defaultdict(_agg_row_init)
    for n in nodes:
        if n.index_name:
            key = (n.index_name, n.node_type)
            _agg_add(by_index[key], n)

    index_rows = []
    for (index_name, node_type), agg in by_index.items():
        index_rows.append({
            "index": index_name,
            "node_type": node_type,
            "count": agg["count"],
            "self_time_ms": agg["self_time_ms"],
            "self_rows": agg["self_rows"],
            "buffers": agg["buffers"],
        })
    index_rows = _finalize_rows(index_rows, denom_ms)

    # Optional: top nodes list (useful for drilling down)
    top_nodes = None
    if include_top_nodes:
        tmp = sorted(nodes, key=lambda n: n.self_ms, reverse=True)[:top_n]
        top_nodes = []
        for n in tmp:
            table = None
            if n.relation:
                table = f"{n.schema}.{n.relation}" if n.schema else n.relation
            top_nodes.append({
                "node_type": n.node_type,
                "table": table,
                "index": n.index_name,
                "self_time_ms": n.self_ms,
                "self_time_pct": 100.0 * n.self_ms / denom_ms,
                "self_rows": n.self_rows,
                **n.buffers.to_dict(),
            })

    return {
        "summary": {
            "execution_time_ms": execution_time_ms,
            "planning_time_ms": planning_time_ms,
            "denominator_ms_for_pct": denom_ms,
            "node_count": len(nodes),

            # NEW
            "total_time_ms": total_time_ms,
            "planning_ratio": planning_ratio,     # 0..1
            "execution_ratio": execution_ratio,   # 0..1
            "dominant_factor": dominant_factor,   # planner_dominated|execution_dominated|io_dominated|cpu_dominated
            "dominant_scores": scores,            # debug
            "dominant_explain": dominant_explain,
            "buffers_total": buf_sum,
            "buffers_read_ratio": read_ratio,
        },
        "by_node_type": node_type_rows,
        "by_table": table_rows,
        "by_index": index_rows,
        "top_nodes": top_nodes,
    }


