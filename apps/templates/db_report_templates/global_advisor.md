## {{ chapter_name }}

{% if result.status == 'error' %}
> Global Advisor could not run: {{ result.message or 'Unknown error.' }}
{% else %}
{{ summary.advisor_message or 'Global Advisor completed.' }}

| Metric | Value |
|---|---:|
| Recommendations | {{ summary.total or 0 }} |
| High priority | {{ summary.priority_counts.get('HIGH', 0) if summary.priority_counts else 0 }} |
| Medium priority | {{ summary.priority_counts.get('MEDIUM', 0) if summary.priority_counts else 0 }} |
| Low priority | {{ summary.priority_counts.get('LOW', 0) if summary.priority_counts else 0 }} |
| High risk | {{ summary.high_risk or 0 }} |
| Manual review required | {{ summary.manual_review_required or 0 }} |
| Quick wins | {{ summary.quick_wins_count or 0 }} |
| Checks failed | {{ summary.execution.checks_failed if summary.execution else errors|length }} |

{% if errors %}
### Checks that could not be completed

{% for error in errors %}
- **{{ error.recommendation_id or 'Unknown check' }}** — {{ error.error|string|replace('\n', ' ') }}
{% endfor %}
{% endif %}

{% if recommendations %}
### Prioritized recommendations

{% for rec in recommendations %}
#### {{ loop.index }}. {{ rec.label or rec.recommendation_id or 'Recommendation' }}

- **Priority:** {{ rec.priority or 'UNKNOWN' }} (rank {{ rec.rank }})
- **Team:** {{ rec.team or 'OPS' }}
- **Risk:** {{ rec.risk_level or 'UNKNOWN' }}
- **Category:** {{ rec.category_id or 'OTHER' }}
- **Target:** {{ rec.object_name or rec.table_name or rec.index_name or 'Database-wide' }}
- **Action:** {{ rec.action_type or 'REVIEW_ONLY' }} / {{ rec.action_safety or 'UNKNOWN' }}

{% if rec.why_it_matters %}
**Why it matters:** {{ rec.why_it_matters }}
{% endif %}

{% if rec.recommendation_note %}
**Finding:** {{ rec.recommendation_note }}
{% endif %}

{% if rec.fix_strategy %}
**Recommended approach:** {{ rec.fix_strategy }}
{% endif %}

{% if rec.expected_benefit %}
**Expected benefit:** {{ rec.expected_benefit }}
{% endif %}

{% if rec.improvement_sql %}
**Suggested SQL — review before execution:**

```sql
{{ rec.improvement_sql }}
```
{% endif %}

{% endfor %}
{% else %}
No active Global Advisor recommendation was found.
{% endif %}
{% endif %}

---
