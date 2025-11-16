{% macro union_with_nulls(table_cols_map, all_cols) %}
  {% for t in table_cols_map %}
    SELECT
    {% for c in all_cols %}
      {% if c in table_cols_map[t] %}
        {{ c }}
      {% else %}
        NULL as {{ c }}
      {% endif %}
      {% if not loop.last %},{% endif %}
    {% endfor %}
    FROM {{ t }}
    {% if not loop.last %} UNION ALL {% endif %}
  {% endfor %}
{% endmacro %}