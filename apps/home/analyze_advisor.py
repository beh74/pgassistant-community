"""Index recommendation engine based on PostgreSQL JSON execution plans."""

import json
import re
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

from . import database
from . import alalyze_advisor_helpers as helpers


def analyze_plan_for_safe_indexes(
    plan_json: Any,
    session: Dict[str, Any],
    queryid: int | str | None = None,
) -> Dict[str, Any]:
    """
    Analyzes an EXPLAIN ANALYZE FORMAT JSON plan and proposes "safe only" indexes.
    Also reports the access path already used (Seq Scan, Index Scan, etc.)
    as well as the index used when applicable.
    """
    db_config = helpers.get_db_config_from_session(session)
    con, message = database.connectdb(db_config)

    if con is None:
        return {
            "ok": False,
            "message": message or "Unable to connect to database.",
            "recommendations": [],
            "scan_findings": [],
            "join_findings": [],
            "query_stats": None,
        }

    try:
        parsed_plan = helpers.normalize_plan_json(plan_json)
        root = helpers.extract_root_plan(parsed_plan)

        scan_findings: List[helpers.ScanFinding] = []
        join_findings: List[helpers.JoinFinding] = []
        order_by_findings: List[helpers.OrderByFinding] = []
        group_by_findings: List[helpers.GroupByFinding] = []
        alias_map: Dict[str, Dict[str, str]] = {}

        helpers.collect_relation_aliases(root, alias_map)
        helpers.walk_plan_collect_findings(
            root,
            scan_findings,
            join_findings,
            order_by_findings=order_by_findings,
            group_by_findings=group_by_findings,
        )

        query_stats = helpers.load_query_stats(con, queryid) if queryid is not None else None

        recommendations: List[helpers.Recommendation] = []

        # ------------------------------------------------------------
        # Scan-based recommendations / observations
        # ------------------------------------------------------------
        for finding in scan_findings:
            meta = helpers.load_table_meta(con, finding.schema, finding.table)
            if meta is None:
                recommendations.append(
                    helpers.Recommendation(
                        schema=finding.schema,
                        table=finding.table,
                        confidence="none",
                        reason="Could not load table metadata.",
                        node_type=finding.node_type,
                        access_path=finding.node_type,
                        used_index_name=finding.index_name,
                        used_index_def=None,
                        index_cond=finding.index_cond,
                        recheck_cond=finding.recheck_cond,
                        filter_expr=finding.filter_expr,
                        row_estimation_reason=helpers.build_row_estimation_reason(
                            finding.plan_rows,
                            finding.actual_rows,
                        ),
                    )
                )
                continue

            rec = evaluate_scan_candidate(con, finding, meta, query_stats)
            recommendations.append(rec)

        # ------------------------------------------------------------
        # ORDER BY / Sort-based recommendations
        # ------------------------------------------------------------
        for finding in order_by_findings:
            meta = helpers.load_table_meta(con, finding.schema, finding.table)
            if meta is None:
                recommendations.append(
                    helpers.Recommendation(
                        schema=finding.schema,
                        table=finding.table,
                        confidence="none",
                        reason="Could not load table metadata for ORDER BY analysis.",
                        node_type=finding.node_type,
                        access_path=finding.child_node_type,
                        used_index_name=finding.child_index_name,
                        index_cond=finding.child_index_cond,
                        recheck_cond=finding.child_recheck_cond,
                        filter_expr=finding.child_filter_expr,
                        recommendation_type="order_by_index_observation",
                    )
                )
                continue

            recommendations.append(
                evaluate_order_by_candidate(con, finding, meta, query_stats)
            )

        # ------------------------------------------------------------
        # GROUP BY / Aggregate-based recommendations
        # ------------------------------------------------------------
        for finding in group_by_findings:
            meta = helpers.load_table_meta(con, finding.schema, finding.table)
            if meta is None:
                recommendations.append(
                    helpers.Recommendation(
                        schema=finding.schema,
                        table=finding.table,
                        confidence="none",
                        reason="Could not load table metadata for GROUP BY analysis.",
                        node_type=finding.node_type,
                        access_path=finding.child_node_type,
                        used_index_name=finding.child_index_name,
                        index_cond=finding.child_index_cond,
                        recheck_cond=finding.child_recheck_cond,
                        filter_expr=finding.child_filter_expr,
                        recommendation_type="group_by_index_observation",
                    )
                )
                continue

            recommendations.append(
                evaluate_group_by_candidate(con, finding, meta, query_stats)
            )

        # ------------------------------------------------------------
        # Join-based recommendations
        # ------------------------------------------------------------
        for join in join_findings:
            recommendations.extend(
                evaluate_join_candidate(con, join, alias_map, query_stats)
            )

        # ------------------------------------------------------------
        # Safety pass: if candidate columns exist, always try to attach stats
        # ------------------------------------------------------------
        for rec in recommendations:
            if rec.candidate_columns and not rec.stats_reason:
                rec.stats_reason = load_candidate_stats_reason(
                    con,
                    rec.schema,
                    rec.table,
                    rec.candidate_columns,
                )

        actionable_recommendations = [
            r for r in recommendations
            if r.confidence not in {"none", "info"} and r.create_index_sql
        ]
        observations = [
            r for r in recommendations
            if r.confidence in {"none", "info"} or not r.create_index_sql
        ]

        return {
            "ok": True,
            "message": "Plan analyzed successfully.",
            # Backward compatible: keep the full list for existing UI code.
            "recommendations": [asdict(r) for r in recommendations],
            # New preferred fields: show actionable_recommendations to users, and
            # keep observations collapsed/debug-only to avoid noisy "no index" cards.
            "actionable_recommendations": [asdict(r) for r in actionable_recommendations],
            "observations": [asdict(r) for r in observations],
            "scan_findings": [asdict(f) for f in scan_findings],
            "join_findings": [asdict(j) for j in join_findings],
            "order_by_findings": [asdict(o) for o in order_by_findings],
            "group_by_findings": [asdict(g) for g in group_by_findings],
            "query_stats": asdict(query_stats) if query_stats else None,
        }

    finally:
        try:
            con.close()
        except Exception:
            pass


# --------------------------------------------------------------------
# Recommendation engine
# --------------------------------------------------------------------

def load_candidate_stats_reason(
    con,
    schema: str,
    table: str,
    candidate_columns: Optional[List[str]],
) -> Optional[str]:
    """Build a compact pg_stats explanation for candidate index columns."""
    if not candidate_columns:
        return None

    return helpers.build_candidate_columns_stats_reason(
        con,
        schema,
        table,
        candidate_columns,
    )


def evaluate_scan_candidate(
    con,
    finding: helpers.ScanFinding,
    meta: helpers.TableMeta,
    query_stats: Optional[helpers.QueryStats] = None,
) -> helpers.Recommendation:
    """Dispatch scan analysis to the Seq Scan or indexed-path evaluator."""
    if finding.node_type == "Seq Scan":
        return evaluate_seq_scan_candidate(con, finding, meta, query_stats)

    if finding.node_type in {"Index Scan", "Index Only Scan", "Bitmap Heap Scan"}:
        return evaluate_indexed_scan_candidate(con, finding, meta, query_stats)

    return helpers.Recommendation(
        schema=finding.schema,
        table=finding.table,
        confidence="none",
        reason=f"Unsupported scan type for advisor: {finding.node_type}",
        node_type=finding.node_type,
        access_path=finding.node_type,
        used_index_name=finding.index_name,
        index_cond=finding.index_cond,
        recheck_cond=finding.recheck_cond,
        filter_expr=finding.filter_expr,
    )


def evaluate_indexed_scan_candidate(
    con,
    finding: helpers.ScanFinding,
    meta: helpers.TableMeta,
    query_stats: Optional[helpers.QueryStats] = None,
) -> helpers.Recommendation:
    """
    Also analyzes scans that already use an index.

    Target case:
      Index Scan using idx_a on t
        Index Cond: (a = 42)
        Filter: (b = 'x')
        Rows Removed by Filter: 100000

    The previous advisor stopped at "an index is already used". Here we check whether
    a more selective composite index, for example (a, b), could reduce the number
    of tuples visited and later filtered by the heap/executor.
    """
    index_predicates: List[Dict[str, str]] = []
    filter_predicates: List[Dict[str, str]] = []

    if finding.index_cond:
        index_predicates = helpers.extract_simple_filter_predicates(
            finding.index_cond,
            alias=finding.alias,
            table=finding.table,
        )

    # Bitmap Heap Scan peut porter la condition indexée dans Recheck Cond.
    if not index_predicates and finding.recheck_cond:
        index_predicates = helpers.extract_simple_filter_predicates(
            finding.recheck_cond,
            alias=finding.alias,
            table=finding.table,
        )

    if finding.filter_expr:
        filter_predicates = helpers.extract_simple_filter_predicates(
            finding.filter_expr,
            alias=finding.alias,
            table=finding.table,
        )

    all_predicates = helpers.merge_simple_predicates(index_predicates, filter_predicates)
    candidate_columns = helpers.reorder_index_candidate_columns(
        con,
        finding.schema,
        finding.table,
        all_predicates,
    ) if all_predicates else []

    stats_reason = (
        helpers.build_candidate_predicates_stats_reason(
            con,
            finding.schema,
            finding.table,
            all_predicates,
        )
        if all_predicates
        else None
    )

    used_index_def = helpers.find_index_definition(meta.indexes, finding.index_name)
    used_index_columns = helpers.find_index_columns(meta.indexes, finding.index_name)

    row_gap_flag, row_gap_reason = helpers.has_large_row_estimation_gap(
        finding.actual_rows,
        finding.plan_rows,
        threshold=3.0,
    )

    if finding.index_name:
        reason = f'{finding.node_type} already in use via index "{finding.index_name}".'
    else:
        reason = f"{finding.node_type} already in use."

    post_index_filter_fraction = helpers.estimate_post_index_filter_fraction(finding)
    post_index_filter_reason = helpers.build_post_index_filter_reason(finding)

    if not finding.filter_expr:
        reason += " No residual Filter is present, so the current index already supports the visible scan predicates."
        if row_gap_flag:
            reason += " A notable planner row-estimation gap was observed for this indexed access path."

        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=reason,
            node_type=finding.node_type,
            access_path=finding.node_type,
            used_index_name=finding.index_name,
            used_index_def=used_index_def,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns or None,
            stats_reason=stats_reason,
            row_estimation_reason=row_gap_reason,
        )

    if not filter_predicates:
        reason += " A residual Filter exists, but it is not simple enough for a safe better-index recommendation."
        if post_index_filter_reason:
            reason += " " + post_index_filter_reason

        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=reason,
            node_type=finding.node_type,
            access_path=finding.node_type,
            used_index_name=finding.index_name,
            used_index_def=used_index_def,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns or None,
            stats_reason=stats_reason,
            row_estimation_reason=row_gap_reason,
        )

    if not candidate_columns:
        reason += " Could not build a reliable candidate column list from Index Cond + Filter."
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=reason,
            node_type=finding.node_type,
            access_path=finding.node_type,
            used_index_name=finding.index_name,
            used_index_def=used_index_def,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            row_estimation_reason=row_gap_reason,
        )

    matched_index = helpers.find_equivalent_index(meta.indexes, candidate_columns)
    if matched_index:
        reason += (
            f' A candidate index on ({", ".join(candidate_columns)}) is already covered '
            f'by existing index "{matched_index}".'
        )
        if post_index_filter_reason:
            reason += " " + post_index_filter_reason

        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=reason,
            node_type=finding.node_type,
            access_path=finding.node_type,
            used_index_name=finding.index_name,
            used_index_def=used_index_def,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns,
            existing_index_match=matched_index,
            stats_reason=stats_reason,
            row_estimation_reason=row_gap_reason,
        )

    adds_filter_columns = helpers.candidate_adds_columns_to_used_index(
        used_index_columns,
        candidate_columns,
    )

    filter_is_selective = (
        post_index_filter_fraction is not None
        and post_index_filter_fraction <= 0.30
        and finding.rows_removed_by_filter >= max(100.0, finding.actual_rows * 2.0)
    )

    meaningful_runtime = finding.actual_total_time >= 1.0 or helpers.is_high_workload(query_stats)

    if adds_filter_columns and filter_is_selective and meaningful_runtime:
        reason += (
            " The current index is useful, but the residual Filter is still highly selective after "
            "the index access; a composite index that includes the filter column(s) may reduce heap "
            "visits and improve the execution plan."
        )
        if post_index_filter_reason:
            reason += " " + post_index_filter_reason

        confidence = "review"
        if len(candidate_columns) == 1:
            confidence = "safe"

        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence=confidence,
            reason=reason,
            node_type=finding.node_type,
            access_path=finding.node_type,
            used_index_name=finding.index_name,
            used_index_def=used_index_def,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns,
            create_index_sql=helpers.build_create_index_sql(
                finding.schema,
                finding.table,
                candidate_columns,
            ),
            stats_reason=stats_reason,
            row_estimation_reason=row_gap_reason,
        )

    reason += " Existing indexed access path does not show enough residual-filter waste for a better-index recommendation."
    if post_index_filter_reason:
        reason += " " + post_index_filter_reason
    if row_gap_flag:
        reason += " A notable planner row-estimation gap was observed."

    return helpers.Recommendation(
        schema=finding.schema,
        table=finding.table,
        confidence="none",
        reason=reason,
        node_type=finding.node_type,
        access_path=finding.node_type,
        used_index_name=finding.index_name,
        used_index_def=used_index_def,
        index_cond=finding.index_cond,
        recheck_cond=finding.recheck_cond,
        filter_expr=finding.filter_expr,
        candidate_columns=candidate_columns or None,
        stats_reason=stats_reason,
        row_estimation_reason=row_gap_reason,
    )

def evaluate_seq_scan_candidate(
    con,
    finding: helpers.ScanFinding,
    meta: helpers.TableMeta,
    query_stats: Optional[helpers.QueryStats] = None,
) -> helpers.Recommendation:
    """Evaluate whether a sequential scan has a safe index opportunity."""
    candidate_columns: List[str] = []
    predicates: List[Dict[str, str]] = []

    if finding.filter_expr:
        predicates = helpers.extract_simple_filter_predicates(
            finding.filter_expr,
            alias=finding.alias,
            table=finding.table,
        )

        if predicates:
            candidate_columns = helpers.reorder_index_candidate_columns(
                con,
                finding.schema,
                finding.table,
                predicates,
            )

    stats_reason = (
        helpers.build_candidate_predicates_stats_reason(
            con,
            finding.schema,
            finding.table,
            predicates,
        )
        if predicates
        else None
    )

    row_gap_flag, row_gap_reason = helpers.has_large_row_estimation_gap(
        finding.actual_rows,
        finding.plan_rows,
    )

    if not finding.filter_expr:
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=helpers.build_no_filter_seq_scan_reason(finding, meta),
            node_type=finding.node_type,
            access_path=finding.node_type,
            row_estimation_reason=row_gap_reason,
            used_index_name=finding.index_name,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            candidate_columns=None,
            stats_reason=None,
        )

    if helpers.is_small_table(meta):
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=(
                "Table is small; sequential scan is usually appropriate and "
                "an automatic index recommendation would not be safe."
            ),
            node_type=finding.node_type,
            access_path=finding.node_type,
            used_index_name=finding.index_name,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns or None,
            stats_reason=stats_reason,
            row_estimation_reason=row_gap_reason,
        )

    planned_but_not_executed = finding.actual_loops <= 0

    # Important: a node with Actual Loops = 0 did not run because an upstream
    # node returned no rows. Its Actual Total Time is therefore 0, but the plan
    # can still reveal a poor access path that would hurt as soon as the outer
    # side returns rows. Do not classify it as "already very fast".
    if (
        finding.actual_total_time < 1.0
        and not planned_but_not_executed
        and not helpers.is_high_workload(query_stats)
    ):
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason="Scan is already very fast and workload is not significant enough.",
            node_type=finding.node_type,
            access_path=finding.node_type,
            used_index_name=finding.index_name,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns or None,
            stats_reason=stats_reason,
            row_estimation_reason=row_gap_reason,
        )

    selected_fraction = helpers.estimate_selected_fraction(finding)
    selected_fraction_source = "execution stats"

    if selected_fraction is None and planned_but_not_executed:
        selected_fraction = helpers.estimate_planned_selected_fraction(finding, meta)
        selected_fraction_source = "planner estimates because Actual Loops = 0"

    if selected_fraction is None:
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason="Unable to estimate filter selectivity safely from execution stats or planner estimates.",
            node_type=finding.node_type,
            access_path=finding.node_type,
            used_index_name=finding.index_name,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns or None,
            stats_reason=stats_reason,
            row_estimation_reason=row_gap_reason,
        )

    if planned_but_not_executed and not helpers.is_planned_scan_potentially_expensive(finding, meta):
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=(
                "Scan node was planned but not executed (Actual Loops = 0), and its planned "
                "cost is not high enough for a safe index recommendation."
            ),
            node_type=finding.node_type,
            access_path=finding.node_type,
            used_index_name=finding.index_name,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns or None,
            stats_reason=stats_reason,
            row_estimation_reason=row_gap_reason,
        )

    if selected_fraction >= 0.50:
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=(
                f"Filter keeps a very large fraction of rows "
                f"({selected_fraction:.1%}); a sequential scan is likely appropriate."
            ),
            node_type=finding.node_type,
            access_path=finding.node_type,
            used_index_name=finding.index_name,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns or None,
            stats_reason=stats_reason,
            row_estimation_reason=row_gap_reason,
        )

    if not candidate_columns:
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason="Filter is not simple enough for a safe automatic index recommendation.",
            node_type=finding.node_type,
            access_path=finding.node_type,
            used_index_name=finding.index_name,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            candidate_columns=None,
            stats_reason=None,
            row_estimation_reason=row_gap_reason,
        )

    matched_index = helpers.find_equivalent_index(meta.indexes, candidate_columns)
    if matched_index:
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason="An equivalent index already exists.",
            node_type=finding.node_type,
            access_path=finding.node_type,
            used_index_name=finding.index_name,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns,
            existing_index_match=matched_index,
            stats_reason=stats_reason,
            row_estimation_reason=row_gap_reason,
        )

    if helpers.looks_like_prefix_search(finding.filter_expr):
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="review",
            reason=(
                "Prefix LIKE filter detected. An index may help, but operator class / collation "
                "should be verified before recommending it automatically."
            ),
            node_type=finding.node_type,
            access_path=finding.node_type,
            used_index_name=finding.index_name,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns,
            create_index_sql=helpers.build_create_index_sql(
                finding.schema,
                finding.table,
                candidate_columns,
            ),
            stats_reason=stats_reason,
            row_estimation_reason=row_gap_reason,
        )

    if helpers.looks_suspicious_predicate(finding.filter_expr):
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="review",
            reason=(
                "Highly selective filter on a non-small table, but the predicate looks unusual. "
                "Review before creating an index."
            ),
            node_type=finding.node_type,
            access_path=finding.node_type,
            used_index_name=finding.index_name,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns,
            create_index_sql=helpers.build_create_index_sql(
                finding.schema,
                finding.table,
                candidate_columns,
            ),
            stats_reason=stats_reason,
            row_estimation_reason=row_gap_reason,
        )

    if len(candidate_columns) == 1:
        stats = helpers.load_column_stats(con, finding.schema, finding.table, candidate_columns[0])

        if stats is None:
            return helpers.Recommendation(
                schema=finding.schema,
                table=finding.table,
                confidence="review",
                reason=(
                    "Execution suggests a selective filtered scan, but pg_stats are unavailable; "
                    "review before creating the index."
                ),
                node_type=finding.node_type,
                access_path=finding.node_type,
                used_index_name=finding.index_name,
                index_cond=finding.index_cond,
                recheck_cond=finding.recheck_cond,
                filter_expr=finding.filter_expr,
                candidate_columns=candidate_columns,
                create_index_sql=helpers.build_create_index_sql(
                    finding.schema,
                    finding.table,
                    candidate_columns,
                ),
                stats_reason=stats_reason,
                row_estimation_reason=row_gap_reason,
            )

        estimated_selectivity = helpers.estimate_selectivity_from_stats(finding.filter_expr, stats)

        if estimated_selectivity is None:
            return helpers.Recommendation(
                schema=finding.schema,
                table=finding.table,
                confidence="review",
                reason=(
                    "Execution suggests a selective filtered scan, but column statistics do not "
                    "allow a confident selectivity estimate without deeper predicate parsing."
                ),
                node_type=finding.node_type,
                access_path=finding.node_type,
                used_index_name=finding.index_name,
                index_cond=finding.index_cond,
                recheck_cond=finding.recheck_cond,
                filter_expr=finding.filter_expr,
                candidate_columns=candidate_columns,
                create_index_sql=helpers.build_create_index_sql(
                    finding.schema,
                    finding.table,
                    candidate_columns,
                ),
                stats_reason=stats_reason,
                row_estimation_reason=row_gap_reason,
            )

        if estimated_selectivity >= 0.20:
            return helpers.Recommendation(
                schema=finding.schema,
                table=finding.table,
                confidence="review",
                reason=(
                    "Execution was selective, but pg_stats suggest the predicate may not be "
                    "selective enough overall to justify a safe automatic index recommendation."
                ),
                node_type=finding.node_type,
                access_path=finding.node_type,
                used_index_name=finding.index_name,
                index_cond=finding.index_cond,
                recheck_cond=finding.recheck_cond,
                filter_expr=finding.filter_expr,
                candidate_columns=candidate_columns,
                create_index_sql=helpers.build_create_index_sql(
                    finding.schema,
                    finding.table,
                    candidate_columns,
                ),
                stats_reason=(
                    f"{stats_reason} | estimated_selectivity={estimated_selectivity:.1%}"
                    if stats_reason
                    else f"estimated_selectivity={estimated_selectivity:.1%}"
                ),
                row_estimation_reason=row_gap_reason,
            )

        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="safe",
            reason=(
                "Highly selective filtered sequential scan on a non-small table with no equivalent "
                "existing index, confirmed by pg_stats."
            ),
            node_type=finding.node_type,
            access_path=finding.node_type,
            used_index_name=finding.index_name,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns,
            create_index_sql=helpers.build_create_index_sql(
                finding.schema,
                finding.table,
                candidate_columns,
            ),
            stats_reason=(
                f"{stats_reason} | estimated_selectivity={estimated_selectivity:.1%}"
                if stats_reason
                else f"estimated_selectivity={estimated_selectivity:.1%}"
            ),
            row_estimation_reason=row_gap_reason,
        )

    return helpers.Recommendation(
        schema=finding.schema,
        table=finding.table,
        confidence="review",
        reason="Multiple candidate columns detected. Review manually before creating a composite index.",
        node_type=finding.node_type,
        access_path=finding.node_type,
        used_index_name=finding.index_name,
        index_cond=finding.index_cond,
        recheck_cond=finding.recheck_cond,
        filter_expr=finding.filter_expr,
        candidate_columns=candidate_columns,
        create_index_sql=helpers.build_create_index_sql(
            finding.schema,
            finding.table,
            candidate_columns,
        ),
        stats_reason=stats_reason,
        row_estimation_reason=row_gap_reason,
    )


def evaluate_order_by_candidate(
    con,
    finding: helpers.OrderByFinding,
    meta: helpers.TableMeta,
    query_stats: Optional[helpers.QueryStats] = None,
) -> helpers.Recommendation:
    """
    Recommends conservative indexes for simple ORDER BY patterns, especially:

      Limit -> Sort -> Scan
      Sort  -> Scan

    Candidate index order:
      1. equality predicates from Index Cond / Recheck Cond / Filter
      2. ORDER BY columns with their direction

    Range predicates are deliberately not prepended before the ORDER BY key in
    this V1 because they can prevent the index order from satisfying the sort.
    """
    order_columns = helpers.extract_simple_sort_keys(
        finding.sort_key,
        alias=finding.alias,
        table=finding.table,
    )

    base_reason = (
        f"{finding.node_type} above {finding.child_node_type} on "
        f"{finding.schema}.{finding.table}."
    )
    sort_context = helpers.build_sort_context_reason(finding)
    if sort_context:
        base_reason += " " + sort_context + "."

    if not order_columns:
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=(
                base_reason
                + " Sort keys are not simple single-table columns, so no safe ORDER BY index recommendation is emitted."
            ),
            node_type=finding.node_type,
            access_path=finding.child_node_type,
            used_index_name=finding.child_index_name,
            index_cond=finding.child_index_cond,
            recheck_cond=finding.child_recheck_cond,
            filter_expr=finding.child_filter_expr,
            candidate_order_columns=None,
            recommendation_type="order_by_index_observation",
        )

    if helpers.is_small_table(meta):
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=(
                base_reason
                + " Table is small; keeping the explicit sort is usually cheaper than adding a dedicated ORDER BY index."
            ),
            node_type=finding.node_type,
            access_path=finding.child_node_type,
            used_index_name=finding.child_index_name,
            index_cond=finding.child_index_cond,
            recheck_cond=finding.child_recheck_cond,
            filter_expr=finding.child_filter_expr,
            candidate_order_columns=order_columns,
            recommendation_type="order_by_index_observation",
        )

    predicates: List[Dict[str, str]] = []

    for expr in (
        finding.child_index_cond,
        finding.child_recheck_cond,
        finding.child_filter_expr,
    ):
        if not expr:
            continue
        parsed = helpers.extract_simple_filter_predicates(
            expr,
            alias=finding.alias,
            table=finding.table,
        )
        predicates = helpers.merge_simple_predicates(predicates, parsed)

    equality_predicates = [p for p in predicates if p.get("operator") == "="]
    filter_columns = (
        helpers.reorder_index_candidate_columns(
            con,
            finding.schema,
            finding.table,
            equality_predicates,
        )
        if equality_predicates
        else []
    )

    candidate_columns = helpers.merge_columns_with_order(filter_columns, order_columns)

    if not candidate_columns:
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=base_reason + " Could not build a reliable ORDER BY candidate column list.",
            node_type=finding.node_type,
            access_path=finding.child_node_type,
            used_index_name=finding.child_index_name,
            index_cond=finding.child_index_cond,
            recheck_cond=finding.child_recheck_cond,
            filter_expr=finding.child_filter_expr,
            candidate_order_columns=order_columns,
            recommendation_type="order_by_index_observation",
        )

    matched_index = helpers.find_equivalent_index(meta.indexes, candidate_columns)
    if matched_index:
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=(
                base_reason
                + f' Candidate ORDER BY index columns ({", ".join(candidate_columns)}) are already covered by existing index "{matched_index}".'
            ),
            node_type=finding.node_type,
            access_path=finding.child_node_type,
            used_index_name=finding.child_index_name,
            index_cond=finding.child_index_cond,
            recheck_cond=finding.child_recheck_cond,
            filter_expr=finding.child_filter_expr,
            candidate_columns=candidate_columns,
            candidate_order_columns=order_columns,
            existing_index_match=matched_index,
            stats_reason=load_candidate_stats_reason(con, finding.schema, finding.table, candidate_columns),
            recommendation_type="order_by_index_observation",
        )

    sort_is_meaningful = (
        finding.has_limit
        or helpers.sort_spilled_to_disk(finding)
        or finding.actual_total_time >= 1.0
        or helpers.is_high_workload(query_stats)
    )

    if not sort_is_meaningful:
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=(
                base_reason
                + " The sort does not look expensive enough and the workload is not significant enough for an automatic index recommendation."
            ),
            node_type=finding.node_type,
            access_path=finding.child_node_type,
            used_index_name=finding.child_index_name,
            index_cond=finding.child_index_cond,
            recheck_cond=finding.child_recheck_cond,
            filter_expr=finding.child_filter_expr,
            candidate_columns=candidate_columns,
            candidate_order_columns=order_columns,
            stats_reason=load_candidate_stats_reason(con, finding.schema, finding.table, candidate_columns),
            recommendation_type="order_by_index_observation",
        )

    if finding.has_limit and filter_columns:
        confidence = "safe"
        reason = (
            base_reason
            + " ORDER BY with LIMIT follows equality predicate(s) on the same table. "
            "A composite B-tree index with equality column(s) first and ORDER BY column(s) next may let PostgreSQL avoid the explicit Sort and return the first rows faster."
        )
    elif finding.has_limit:
        confidence = "review"
        reason = (
            base_reason
            + " ORDER BY with LIMIT can often benefit from an index matching the sort key, but there is no simple equality predicate to narrow the scan first. Review selectivity and projected columns before creating it."
        )
    elif helpers.sort_spilled_to_disk(finding):
        confidence = "review"
        reason = (
            base_reason
            + " The sort spilled to disk. An index matching the ORDER BY keys may avoid or reduce the explicit Sort, but review the query shape before creating a dedicated index."
        )
    else:
        confidence = "review"
        reason = (
            base_reason
            + " The plan performs an explicit sort on a non-small table. An index matching the ORDER BY keys may help, especially if the query is frequent or latency-sensitive."
        )

    stats_reason = load_candidate_stats_reason(
        con,
        finding.schema,
        finding.table,
        candidate_columns,
    )

    return helpers.Recommendation(
        schema=finding.schema,
        table=finding.table,
        confidence=confidence,
        reason=reason,
        node_type=finding.node_type,
        access_path=finding.child_node_type,
        used_index_name=finding.child_index_name,
        index_cond=finding.child_index_cond,
        recheck_cond=finding.child_recheck_cond,
        filter_expr=finding.child_filter_expr,
        candidate_columns=candidate_columns,
        candidate_order_columns=order_columns,
        create_index_sql=helpers.build_create_index_sql_with_order(
            finding.schema,
            finding.table,
            filter_columns,
            order_columns,
        ),
        stats_reason=stats_reason,
        row_estimation_reason=helpers.build_row_estimation_reason(
            finding.child_plan_rows,
            finding.child_actual_rows,
        ),
        recommendation_type="order_by_limit_index" if finding.has_limit else "order_by_index",
    )


def evaluate_group_by_candidate(
    con,
    finding: helpers.GroupByFinding,
    meta: helpers.TableMeta,
    query_stats: Optional[helpers.QueryStats] = None,
) -> helpers.Recommendation:
    """Evaluate simple GROUP BY patterns that may benefit from ordered access."""
    group_columns = helpers.extract_simple_group_keys(
        finding.group_key,
        alias=finding.alias,
        table=finding.table,
    )

    base_reason = (
        f"{finding.node_type} GROUP BY above {finding.child_node_type} on "
        f"{finding.schema}.{finding.table}."
    )
    group_context = helpers.build_group_by_context_reason(finding)
    if group_context:
        base_reason += " " + group_context + "."

    if not group_columns:
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=(
                base_reason
                + " Group keys are not simple single-table columns, so no safe GROUP BY index recommendation is emitted."
            ),
            node_type=finding.node_type,
            access_path=finding.child_node_type,
            used_index_name=finding.child_index_name,
            index_cond=finding.child_index_cond,
            recheck_cond=finding.child_recheck_cond,
            filter_expr=finding.child_filter_expr,
            candidate_group_columns=None,
            recommendation_type="group_by_index_observation",
        )

    if helpers.is_small_table(meta):
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=(
                base_reason
                + " Table is small; adding a dedicated GROUP BY index is usually not worth it."
            ),
            node_type=finding.node_type,
            access_path=finding.child_node_type,
            used_index_name=finding.child_index_name,
            index_cond=finding.child_index_cond,
            recheck_cond=finding.child_recheck_cond,
            filter_expr=finding.child_filter_expr,
            candidate_columns=group_columns,
            candidate_group_columns=group_columns,
            recommendation_type="group_by_index_observation",
        )

    matched_index = helpers.find_equivalent_index(meta.indexes, group_columns)
    if matched_index:
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=(
                base_reason
                + f' GROUP BY columns ({", ".join(group_columns)}) are already covered by existing index "{matched_index}".'
            ),
            node_type=finding.node_type,
            access_path=finding.child_node_type,
            used_index_name=finding.child_index_name,
            index_cond=finding.child_index_cond,
            recheck_cond=finding.child_recheck_cond,
            filter_expr=finding.child_filter_expr,
            candidate_columns=group_columns,
            candidate_group_columns=group_columns,
            existing_index_match=matched_index,
            stats_reason=load_candidate_stats_reason(con, finding.schema, finding.table, group_columns),
            recommendation_type="group_by_index_observation",
        )

    group_is_meaningful = (
        finding.sort_method is not None
        or helpers.group_by_spilled_to_disk(finding)
        or finding.actual_total_time >= 1.0
        or helpers.is_high_workload(query_stats)
    )

    if not group_is_meaningful:
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason=(
                base_reason
                + " The GROUP BY does not look expensive enough and the workload is not significant enough for an automatic index recommendation."
            ),
            node_type=finding.node_type,
            access_path=finding.child_node_type,
            used_index_name=finding.child_index_name,
            index_cond=finding.child_index_cond,
            recheck_cond=finding.child_recheck_cond,
            filter_expr=finding.child_filter_expr,
            candidate_columns=group_columns,
            candidate_group_columns=group_columns,
            stats_reason=load_candidate_stats_reason(con, finding.schema, finding.table, group_columns),
            recommendation_type="group_by_index_observation",
        )

    if helpers.group_by_spilled_to_disk(finding):
        reason = (
            base_reason
            + " The sort used by GROUP BY spilled to disk. A B-tree index matching the GROUP BY columns may avoid or reduce the sort, but review the query shape before creating it."
        )
    elif finding.sort_method is not None:
        reason = (
            base_reason
            + " GROUP BY is fed by an explicit sort. A B-tree index matching the GROUP BY columns may let PostgreSQL read rows in grouped order and avoid that sort."
        )
    else:
        reason = (
            base_reason
            + " The plan groups rows on a non-small table. An index matching the GROUP BY columns may help, especially if this query is frequent; PostgreSQL may still prefer HashAggregate depending on data distribution."
        )

    return helpers.Recommendation(
        schema=finding.schema,
        table=finding.table,
        confidence="review",
        reason=reason,
        node_type=finding.node_type,
        access_path=finding.child_node_type,
        used_index_name=finding.child_index_name,
        index_cond=finding.child_index_cond,
        recheck_cond=finding.child_recheck_cond,
        filter_expr=finding.child_filter_expr,
        candidate_columns=group_columns,
        candidate_group_columns=group_columns,
        create_index_sql=helpers.build_create_index_sql(
            finding.schema,
            finding.table,
            group_columns,
        ),
        stats_reason=load_candidate_stats_reason(con, finding.schema, finding.table, group_columns),
        row_estimation_reason=helpers.build_row_estimation_reason(
            finding.child_plan_rows,
            finding.child_actual_rows,
        ),
        recommendation_type="group_by_index",
    )


def evaluate_join_candidate(
    con,
    join: helpers.JoinFinding,
    alias_map: Dict[str, Dict[str, str]],
    query_stats: Optional[helpers.QueryStats] = None,
) -> List[helpers.Recommendation]:
    """Evaluate whether join columns on non-small tables lack supporting indexes."""
    recommendations: List[helpers.Recommendation] = []

    if not all([join.left_alias, join.left_column, join.right_alias, join.right_column]):
        return recommendations

    left_rel = alias_map.get(join.left_alias)
    right_rel = alias_map.get(join.right_alias)

    if not left_rel or not right_rel:
        return recommendations

    left_meta = helpers.load_table_meta(con, left_rel["schema"], left_rel["table"])
    right_meta = helpers.load_table_meta(con, right_rel["schema"], right_rel["table"])

    if left_meta is None or right_meta is None:
        return recommendations

    if not helpers.is_small_table(left_meta):
        existing_left = helpers.find_equivalent_index(left_meta.indexes, [join.left_column])
        if not existing_left:
            left_stats_reason = load_candidate_stats_reason(
                con,
                left_meta.schema,
                left_meta.table,
                [join.left_column],
            )

            recommendations.append(
                helpers.Recommendation(
                    schema=left_meta.schema,
                    table=left_meta.table,
                    confidence="review",
                    reason=(
                        f"{join.cond_type} on {join.left_alias}.{join.left_column} = "
                        f"{join.right_alias}.{join.right_column}. "
                        "No equivalent single-column index found on this non-small table; "
                        "review whether a join-supporting index would help."
                    ),
                    node_type=join.join_node_type,
                    access_path=join.join_node_type,
                    filter_expr=join.cond_expr,
                    candidate_columns=[join.left_column],
                    create_index_sql=helpers.build_create_index_sql(
                        left_meta.schema,
                        left_meta.table,
                        [join.left_column],
                    ),
                    stats_reason=left_stats_reason,
                )
            )

    if not helpers.is_small_table(right_meta):
        existing_right = helpers.find_equivalent_index(right_meta.indexes, [join.right_column])
        if not existing_right:
            right_stats_reason = load_candidate_stats_reason(
                con,
                right_meta.schema,
                right_meta.table,
                [join.right_column],
            )

            recommendations.append(
                helpers.Recommendation(
                    schema=right_meta.schema,
                    table=right_meta.table,
                    confidence="review",
                    reason=(
                        f"{join.cond_type} on {join.left_alias}.{join.left_column} = "
                        f"{join.right_alias}.{join.right_column}. "
                        "No equivalent single-column index found on this non-small table; "
                        "review whether a join-supporting index would help."
                    ),
                    node_type=join.join_node_type,
                    access_path=join.join_node_type,
                    filter_expr=join.cond_expr,
                    candidate_columns=[join.right_column],
                    create_index_sql=helpers.build_create_index_sql(
                        right_meta.schema,
                        right_meta.table,
                        [join.right_column],
                    ),
                    stats_reason=right_stats_reason,
                )
            )

    return recommendations



def get_columns_statistics(result: Dict[str, Any]) -> List[str]:
    """Extract human-readable column statistics summaries from recommendations."""
    stats: List[str] = []

    for rec in result.get("recommendations", []):
        stats_reason = rec.get("stats_reason")

        if not stats_reason:
            continue

        schema = rec.get("schema")
        table = rec.get("table")

        stats.append(
            f"{schema}.{table}: column with statistics -> {stats_reason}"
        )

    return stats

# --------------------------------------------------------------------
# Pretty printer / debug helper
# --------------------------------------------------------------------

def pretty_print_analysis(result: Dict[str, Any]) -> None:
    """Print a verbose text dump of advisor findings for debugging."""
    print(result["message"])

    if result.get("scan_findings"):
        print("=" * 80)
        print("SCAN FINDINGS")
        for finding in result["scan_findings"]:
            print("-" * 80)
            print(f"{finding['schema']}.{finding['table']} ({finding['node_type']})")
            if finding.get("alias"):
                print(f"alias: {finding['alias']}")
            if finding.get("index_name"):
                print(f"index: {finding['index_name']}")
            if finding.get("index_cond"):
                print(f"index_cond: {finding['index_cond']}")
            if finding.get("recheck_cond"):
                print(f"recheck_cond: {finding['recheck_cond']}")
            if finding.get("filter_expr"):
                print(f"filter: {finding['filter_expr']}")
            print(f"actual_rows: {finding['actual_rows']}")
            print(f"plan_rows: {finding['plan_rows']}")
            print(f"actual_total_time: {finding['actual_total_time']}")

    if result.get("order_by_findings"):
        print("=" * 80)
        print("ORDER BY FINDINGS")
        for order_finding in result["order_by_findings"]:
            print("-" * 80)
            print(f"{order_finding['schema']}.{order_finding['table']} ({order_finding['node_type']})")
            print(f"child_access_path: {order_finding['child_node_type']}")
            print(f"has_limit: {order_finding['has_limit']}")
            if order_finding.get("sort_key"):
                print(f"sort_key: {order_finding['sort_key']}")
            if order_finding.get("child_filter_expr"):
                print(f"child_filter: {order_finding['child_filter_expr']}")
            if order_finding.get("child_index_cond"):
                print(f"child_index_cond: {order_finding['child_index_cond']}")
            print(f"actual_total_time: {order_finding['actual_total_time']}")

    if result.get("group_by_findings"):
        print("=" * 80)
        print("GROUP BY FINDINGS")
        for group_finding in result["group_by_findings"]:
            print("-" * 80)
            print(f"{group_finding['schema']}.{group_finding['table']} ({group_finding['node_type']})")
            print(f"child_access_path: {group_finding['child_node_type']}")
            if group_finding.get("strategy"):
                print(f"strategy: {group_finding['strategy']}")
            if group_finding.get("group_key"):
                print(f"group_key: {group_finding['group_key']}")
            if group_finding.get("child_filter_expr"):
                print(f"child_filter: {group_finding['child_filter_expr']}")
            if group_finding.get("child_index_cond"):
                print(f"child_index_cond: {group_finding['child_index_cond']}")
            print(f"actual_total_time: {group_finding['actual_total_time']}")

    if result.get("join_findings"):
        print("=" * 80)
        print("JOIN FINDINGS")
        for join in result["join_findings"]:
            print("-" * 80)
            print(f"{join['join_node_type']} / {join['join_type']}")
            print(f"{join['cond_type']}: {join['cond_expr']}")
            print(
                f"left: {join.get('left_alias')}.{join.get('left_column')} | "
                f"right: {join.get('right_alias')}.{join.get('right_column')}"
            )
            print(f"actual_rows: {join['actual_rows']}")
            print(f"plan_rows: {join['plan_rows']}")
            print(f"actual_total_time: {join['actual_total_time']}")

    print("=" * 80)
    print("RECOMMENDATIONS")
    for rec in result["recommendations"]:
        print("-" * 80)
        print(f"{rec['schema']}.{rec['table']}")
        print(f"confidence: {rec['confidence']}")
        if rec.get("node_type"):
            print(f"node_type: {rec['node_type']}")
        if rec.get("access_path"):
            print(f"access_path: {rec['access_path']}")
        print(f"reason: {rec['reason']}")
        if rec.get("used_index_name"):
            print(f"used_index: {rec['used_index_name']}")
        if rec.get("index_cond"):
            print(f"index_cond: {rec['index_cond']}")
        if rec.get("recheck_cond"):
            print(f"recheck_cond: {rec['recheck_cond']}")
        if rec.get("filter_expr"):
            print(f"filter: {rec['filter_expr']}")
        if rec.get("candidate_columns"):
            print(f"columns: {rec['candidate_columns']}")
        if rec.get("candidate_order_columns"):
            print(f"order_columns: {rec['candidate_order_columns']}")
        if rec.get("candidate_group_columns"):
            print(f"group_columns: {rec['candidate_group_columns']}")
        if rec.get("recommendation_type"):
            print(f"recommendation_type: {rec['recommendation_type']}")
        if rec.get("existing_index_match"):
            print(f"existing_index: {rec['existing_index_match']}")
        if rec.get("stats_reason"):
            print(f"stats: {rec['stats_reason']}")
        if rec.get("create_index_sql"):
            print(f"sql: {rec['create_index_sql']}")
