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
    Analyse un plan EXPLAIN ANALYZE FORMAT JSON et propose des index "safe only".
    Remonte aussi le chemin d'accès déjà utilisé (Seq Scan, Index Scan, etc.)
    ainsi que l'index utilisé quand applicable.
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
        alias_map: Dict[str, Dict[str, str]] = {}

        helpers.collect_relation_aliases(root, alias_map)
        helpers.walk_plan_collect_findings(root, scan_findings, join_findings)

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

        return {
            "ok": True,
            "message": "Plan analyzed successfully.",
            "recommendations": [asdict(r) for r in recommendations],
            "scan_findings": [asdict(f) for f in scan_findings],
            "join_findings": [asdict(j) for j in join_findings],
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

    used_index_def = helpers.find_index_definition(meta.indexes, finding.index_name)

    row_gap_flag, row_gap_reason = helpers.has_large_row_estimation_gap(
        finding.actual_rows,
        finding.plan_rows,
        threshold=3.0,
    )

    if finding.index_name:
        reason = f'{finding.node_type} already in use via index "{finding.index_name}".'
    else:
        reason = f"{finding.node_type} already in use."

    if row_gap_flag:
        reason += " A notable planner row-estimation gap was observed for this indexed access path."
    else:
        reason += " Planner row estimation for this indexed access path looks acceptable."

    if finding.filter_expr:
        reason += " No additional simple index recommendation is produced by the current advisor logic for this indexed scan."
    else:
        reason += " No additional simple index recommendation is produced."

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
            reason="Sequential scan without filter: an index would not be a safe recommendation.",
            node_type=finding.node_type,
            access_path=finding.node_type,
            row_estimation_reason=row_gap_reason,
            used_index_name=finding.index_name,
            index_cond=finding.index_cond,
            recheck_cond=finding.recheck_cond,
            filter_expr=finding.filter_expr,
            candidate_columns=candidate_columns or None,
            stats_reason=stats_reason,
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

    if finding.actual_total_time < 1.0 and not helpers.is_high_workload(query_stats):
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
    if selected_fraction is None:
        return helpers.Recommendation(
            schema=finding.schema,
            table=finding.table,
            confidence="none",
            reason="Unable to estimate filter selectivity safely from execution stats.",
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


def evaluate_join_candidate(
    con,
    join: helpers.JoinFinding,
    alias_map: Dict[str, Dict[str, str]],
    query_stats: Optional[helpers.QueryStats] = None,
) -> List[helpers.Recommendation]:
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


# --------------------------------------------------------------------
# Pretty printer / debug helper
# --------------------------------------------------------------------

def pretty_print_analysis(result: Dict[str, Any]) -> None:
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
        if rec.get("existing_index_match"):
            print(f"existing_index: {rec['existing_index_match']}")
        if rec.get("stats_reason"):
            print(f"stats: {rec['stats_reason']}")
        if rec.get("create_index_sql"):
            print(f"sql: {rec['create_index_sql']}")