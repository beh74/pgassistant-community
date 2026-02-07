
## {{ chapter_name }}

### SQL source
```sql
{{ sql }}
```

### Results

{% if rows and rows|length > 0 %}
{% set cols = rows[0].keys() %}
| {%- for c in cols -%}{{ c }}{%- if not loop.last -%} | {%- endif -%}{%- endfor -%} |
| {%- for c in cols -%}---{%- if not loop.last -%} | {%- endif -%}{%- endfor -%} |
{%- for r in rows %}
| {%- for c in cols -%}
{{ r[c]|string
       |replace('\n', ' ')        
       |replace('|', '\|')        
}}{%- if not loop.last -%} | {%- endif -%}
{%- endfor -%} |
{%- endfor %}
{% else %}
Empty data
{% endif %}

---

