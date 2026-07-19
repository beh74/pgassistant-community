## {{ chapter_name }}

{% if result.success == false %}
> Index Advisor could not run: {{ result.error or result.message or 'Unknown error.' }}
{% elif result.supported == false %}
> {{ result.message or 'Index Advisor requires PostgreSQL 16 or newer.' }}

- Detected PostgreSQL major version: **{{ result.postgres_major_version or 'unknown' }}**
- Required PostgreSQL major version: **{{ result.required_version or 16 }}**
{% else %}
Index Advisor analyzes generic plans for the highest-ranked workload queries and only reports actionable index candidates.

| Metric | Value |
|---|---:|
| PostgreSQL major version | {{ result.postgres_major_version }} |
| Query limit | {{ result.query_limit or 0 }} |
| Queries planned | {{ summary.queries_planned or 0 }} |
| Queries analyzed | {{ summary.queries_analyzed or 0 }} |
| Internal queries skipped | {{ summary.queries_skipped_internal or 0 }} |
| Queries without recommendation | {{ summary.queries_without_recommendation or 0 }} |
| Queries failed | {{ summary.queries_failed or 0 }} |
| Recommendations | {{ summary.recommendations or 0 }} |
| Actionable recommendations | {{ summary.actionable_recommendations or 0 }} |

{% if query_results %}
### Actionable index recommendations

{% for item in query_results %}
#### Query {{ item.queryid or 'unknown' }}

```sql
{{ item.query or '-- Query text unavailable' }}
```

{% if item.query_stats %}
- **Calls:** {{ item.query_stats.calls or 0 }}
- **Total execution time:** {{ item.query_stats.total_exec_time or 0 }} ms
- **Mean execution time:** {{ item.query_stats.mean_exec_time or 0 }} ms
- **Rows:** {{ item.query_stats.rows or 0 }}
{% endif %}

{% if item.error %}
> Analysis failed for this query: {{ item.error|string|replace('\n', ' ') }}
{% endif %}

{% for rec in item.actionable_recommendations or [] %}
##### {{ loop.index }}. {{ rec.schema }}.{{ rec.table }}

- **Confidence:** {{ (rec.confidence or 'review')|upper }}
- **Type:** {{ (rec.recommendation_type or 'index recommendation')|replace('_', ' ')|title }}
{% if rec.candidate_columns %}
- **Candidate columns:** {{ rec.candidate_columns|join(', ') }}
{% endif %}
{% if rec.candidate_order_columns %}
- **ORDER BY support:** {% for column in rec.candidate_order_columns %}{{ column.column or column.name }} {{ column.direction or '' }}{% if not loop.last %}, {% endif %}{% endfor %}
{% endif %}
{% if rec.candidate_group_columns %}
- **GROUP BY support:** {{ rec.candidate_group_columns|join(', ') }}
{% endif %}

**Reason:** {{ rec.reason or 'No reason provided.' }}

{% if rec.stats_reason %}
**Statistics context:** {{ rec.stats_reason }}
{% endif %}

{% if rec.row_estimation_reason %}
**Row-estimation context:** {{ rec.row_estimation_reason }}
{% endif %}

{% if rec.create_index_sql %}
**Suggested SQL — validate storage, write overhead and naming before execution:**

```sql
{{ rec.create_index_sql }}
```
{% endif %}

{% endfor %}
{% endfor %}
{% else %}
No actionable index recommendation was found for the analyzed workload.
{% endif %}
{% endif %}

---
