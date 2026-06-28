import math


def _to_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def format_duration_ms(value):
    t = _to_float(value, 0.0)

    if t <= 0:
        return "0 ms"

    seconds = t / 1000.0

    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    ms = int(round(t % 1000))

    if h > 0:
        return f"{h} h {m}m {s} s"

    if m > 0:
        return f"{m} m {s} s"

    if s > 0:
        return f"{s} s {ms} ms"

    return f"{ms} ms"


def _log_norm(value, max_value):
    value = max(_to_float(value, 0.0), 0.0)
    max_value = max(_to_float(max_value, 0.0), 0.0)

    if max_value <= 0:
        return 0.0

    return math.log10(value + 1.0) / math.log10(max_value + 1.0)


def normalize_query_row(row):
    normalized = dict(row)

    normalized["query"] = row.get("query", "") or ""
    normalized["queryid"] = str(row.get("queryid")) if row.get("queryid") is not None else None

    normalized["calls"] = _to_int(row.get("calls"), 0)
    normalized["rows"] = _to_int(row.get("rows"), 0)

    normalized["total_exec_time"] = _to_float(row.get("total_exec_time"), 0.0)
    normalized["mean_exec_time"] = _to_float(row.get("mean_exec_time"), 0.0)
    normalized["min_exec_time"] = _to_float(row.get("min_exec_time"), 0.0)
    normalized["max_exec_time"] = _to_float(row.get("max_exec_time"), 0.0)
    normalized["stddev_exec_time"] = _to_float(row.get("stddev_exec_time"), 0.0)

    normalized["shared_blks_hit"] = _to_int(row.get("shared_blks_hit"), 0)
    normalized["shared_blks_read"] = _to_int(row.get("shared_blks_read"), 0)
    normalized["shared_blks_written"] = _to_int(row.get("shared_blks_written"), 0)

    normalized["local_blks_hit"] = _to_int(row.get("local_blks_hit"), 0)
    normalized["local_blks_read"] = _to_int(row.get("local_blks_read"), 0)
    normalized["local_blks_written"] = _to_int(row.get("local_blks_written"), 0)

    normalized["temp_blks_read"] = _to_int(row.get("temp_blks_read"), 0)
    normalized["temp_blks_written"] = _to_int(row.get("temp_blks_written"), 0)

    normalized["total_blks_read"] = _to_int(row.get("total_blks_read"), 0)
    normalized["total_blks_written"] = _to_int(row.get("total_blks_written"), 0)

    normalized["wal_records"] = _to_int(row.get("wal_records"), 0)
    normalized["wal_fpi"] = _to_int(row.get("wal_fpi"), 0)
    normalized["wal_bytes"] = _to_float(row.get("wal_bytes"), 0.0)

    return normalized


def _should_exclude_query(query: str) -> bool:
    normalized = (query or "").strip().lower()

    if not normalized:
        return True

    if normalized.startswith("/* launched by pgassistant */"):
        return True

    excluded_prefixes = (
        "vacuum",
        "analyze",
        "copy",
        "reindex",
        "cluster",
        "refresh materialized view",
        "checkpoint",
        "discard",
        "listen",
        "unlisten",
        "notify",
        "explain",
        "deallocate",

        "create ",
        "alter ",
        "drop ",
        "truncate ",
        "comment on ",
        "grant ",
        "revoke ",
    )

    return normalized.startswith(excluded_prefixes)


def rank_queries(rows):
    if not rows:
        return []

    normalized_rows = [normalize_query_row(row) for row in rows]

    normalized_rows = [
        row for row in normalized_rows
        if not _should_exclude_query(row.get("query", ""))
    ]

    if not normalized_rows:
        return []

    max_mean_time = max(
        (r["mean_exec_time"] for r in normalized_rows),
        default=0.0,
    )

    max_total_time = max(
        (r["total_exec_time"] for r in normalized_rows),
        default=0.0,
    )

    max_calls = max(
        (r["calls"] for r in normalized_rows),
        default=0,
    )

    sum_total_time = sum(
        (r["total_exec_time"] for r in normalized_rows),
        0.0,
    )

    sum_calls = sum(
        (r["calls"] for r in normalized_rows),
        0,
    )

    sum_blks_read = sum(
        (r["total_blks_read"] for r in normalized_rows),
        0,
    )

    sum_shared_reads = sum(
        (r["shared_blks_read"] for r in normalized_rows),
        0,
    )

    ranked = []

    for row in normalized_rows:
        total_time = row["total_exec_time"]
        mean_time = row["mean_exec_time"]
        calls = row["calls"]
        rows_count = row["rows"]

        shared_hit = row["shared_blks_hit"]
        shared_read = row["shared_blks_read"]

        temp_written = row["temp_blks_written"]
        stddev = row["stddev_exec_time"]

        blks_read = row["total_blks_read"]

        total_blocks = shared_hit + shared_read

        cache_hit_ratio = (
            shared_hit / total_blocks * 100.0
            if total_blocks > 0
            else 100.0
        )

        rows_per_call = (
            rows_count / calls
            if calls > 0
            else 0.0
        )

        # ------------------------------------------------------------
        # Workload shares
        # ------------------------------------------------------------
        share_total = (
            total_time / sum_total_time
            if sum_total_time else 0.0
        )

        share_calls = (
            calls / sum_calls
            if sum_calls else 0.0
        )

        share_io = (
            blks_read / sum_blks_read
            if sum_blks_read else 0.0
        )

        cache_miss_share = (
            shared_read / sum_shared_reads
            if sum_shared_reads else 0.0
        )

        norm_share_total = min(share_total / 0.20, 1.0)
        norm_share_calls = min(share_calls / 0.20, 1.0)
        norm_share_io = min(share_io / 0.20, 1.0)
        norm_cache_miss_share = min(cache_miss_share / 0.20, 1.0)

        # ------------------------------------------------------------
        # Score
        # ------------------------------------------------------------
        LOW_CALL_SHARE = 0.001
        LOW_LOAD_SHARE = 0.005
        LOW_TOTAL_TIME_MS = 100

        norm_total_time = _log_norm(total_time, max_total_time)
        norm_calls = _log_norm(calls, max_calls)
        norm_mean = _log_norm(mean_time, max_mean_time)

        # Workload impact first.
        impact_score = 0.0
        impact_score += norm_share_total * 40
        impact_score += norm_total_time * 25
        impact_score += norm_share_calls * 20
        impact_score += norm_calls * 10
        impact_score += norm_mean * 5

        # Technical smells second.
        technical_score = 0.0

        if cache_hit_ratio < 95 and shared_read >= 1000:
            cache_penalty = (
                ((100.0 - cache_hit_ratio) / 100.0)
                * norm_cache_miss_share
            )

            technical_score += cache_penalty * 10

        if norm_share_io > 0 and blks_read >= 1000:
            technical_score += norm_share_io * 5

        if temp_written > 0:
            technical_score += 8

        if mean_time > 0 and stddev > mean_time * 2:
            technical_score += 4

        score = impact_score + technical_score

        # ------------------------------------------------------------
        # Demotion rules
        # ------------------------------------------------------------
        if (
            share_calls < LOW_CALL_SHARE
            and total_time < LOW_TOTAL_TIME_MS
        ):
            score = min(score, 8)

        if (
            share_calls < LOW_CALL_SHARE
            and share_total < LOW_LOAD_SHARE
        ):
            score = min(score, 12)

        if (
            share_total < 0.01
            and share_calls < 0.01
        ):
            score = min(score, 20)

        score = min(score, 100.0)

        # ------------------------------------------------------------
        # Signals
        # ------------------------------------------------------------
        signals = []

        if share_total >= 0.10:
            signals.append("high_load")

        if share_calls >= 0.10:
            signals.append("high_calls")

        if mean_time > 50:
            signals.append("slow")

        if (
            cache_hit_ratio < 95
            and shared_read >= 1000
            and cache_miss_share >= 0.05
        ):
            signals.append("poor_cache")

        if temp_written > 0:
            signals.append("temp_usage")

        if mean_time > 10:
            cv = stddev / mean_time

            if (
                cv > 1.5
                and row.get("max_exec_time", 0) > mean_time * 3
            ):
                signals.append("unstable")

        # ------------------------------------------------------------
        # Reason
        # ------------------------------------------------------------
        reason_parts = []

        if "high_load" in signals:
            reason_parts.append(
                f"High total load ({share_total:.1%} of total time)"
            )

        if "high_calls" in signals:
            reason_parts.append(
                f"Very frequent execution ({share_calls:.1%} of calls)"
            )

        if "slow" in signals:
            reason_parts.append("Slow execution")

        if "poor_cache" in signals:
            reason_parts.append(
                f"Poor cache efficiency ({cache_miss_share:.1%} of cache misses)"
            )

        if "temp_usage" in signals:
            reason_parts.append("Temp file usage")

        if "unstable" in signals:
            reason_parts.append("High execution variance")

        if not reason_parts:
            if share_total >= 0.05:
                reason_parts.append(
                    f"Meaningful total load ({share_total:.1%} of total time)"
                )

            elif calls >= 100:
                reason_parts.append(
                    f"Frequent execution ({calls} calls)"
                )

            else:
                reason_parts.append("Low workload impact")

        # ------------------------------------------------------------
        # Priority level
        # ------------------------------------------------------------
        if score >= 80:
            level = "Critical"

        elif score >= 60:
            level = "High"

        elif score >= 30:
            level = "Medium"

        else:
            level = "Low"

        enriched = dict(row)

        enriched.update({
            "priority_score": round(score, 1),
            "priority_level": level,
            "reason": " + ".join(reason_parts),
            "signals": signals,

            "cache_hit_ratio": round(cache_hit_ratio, 2),
            "cache_miss_share": round(cache_miss_share * 100, 2),

            "rows_per_call": round(rows_per_call, 2),

            "total_exec_time_formatted": format_duration_ms(total_time),

            "share_total_time": round(share_total * 100, 2),
            "share_calls": round(share_calls * 100, 2),
            "share_io": round(share_io * 100, 2),
        })

        ranked.append(enriched)

    ranked.sort(
        key=lambda x: x["priority_score"],
        reverse=True,
    )

    return ranked
