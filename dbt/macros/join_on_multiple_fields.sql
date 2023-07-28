{% macro join_on_multiple_fields(field_list, left_table='a', right_table='b') %}
({% for id in field_list %}
    {% if not loop.first %}and {% endif %}{{left_table}}.{{id}} = {{right_table}}.{{id}}
{% endfor %})
{% endmacro %}
