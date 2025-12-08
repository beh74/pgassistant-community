
- Database hit cache ratio : **{{ rows[0]['ratio']|float }}** {% if rows[0]['ratio']|float >= 99 %}🟢 Excellent{% elif rows[0]['ratio']|float >= 95 %}🟡 Good{% else %}🔴 Needs Attention{% endif %}

---

