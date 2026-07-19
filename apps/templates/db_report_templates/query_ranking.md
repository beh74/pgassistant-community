## {{ chapter_name }}

The ranking prioritizes queries by workload contribution, execution cost, cache efficiency and other tuning signals. It is an investigation aid, not a verdict.

{% if ranked_queries %}
{% for row in ranked_queries %}
### {{ loop.index }}. {{ row.priority_level or 'Unknown' }} priority — score {{ row.priority_score or 0 }}

{% if row.queryid %}**Query ID:** `{{ row.queryid }}`
{% endif %}

```sql
{{ row.query or '-- Query text unavailable' }}
```

**Why it is ranked here:** {{ row.reason or 'No ranking explanation available.' }}

| Metric | Value |
|---|---:|
| Total execution time | {{ row.total_exec_time_formatted or ((row.total_exec_time or 0)|string + ' ms') }} |
| Mean execution time | {{ '%.2f'|format(row.mean_exec_time or 0) }} ms |
| Calls | {{ row.calls or 0 }} |
| Workload time share | {{ '%.2f'|format(row.share_total_time or 0) }}% |
| Calls share | {{ '%.2f'|format(row.share_calls or 0) }}% |
| I/O share | {{ '%.2f'|format(row.share_io or 0) }}% |
| Cache hit ratio | {{ '%.2f'|format(row.cache_hit_ratio or 0) }}% |
| Rows per call | {{ row.rows_per_call or 0 }} |
| Temporary blocks written | {{ row.temp_blks_written or 0 }} |

{% if row.signals %}**Signals:** {{ row.signals|join(', ')|replace('_', ' ') }}
{% endif %}

{% endfor %}
{% else %}
No query could be ranked. Check that `pg_stat_statements` is installed and contains workload statistics.
{% endif %}

---
