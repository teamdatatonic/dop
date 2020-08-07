{% macro is_incremental() -%}
    {% if not dag_run.conf or dag_run.conf.get('full_refresh') != true %}
        true
    {% endif %}
{%- endmacro -%}