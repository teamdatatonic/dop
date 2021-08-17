{% macro partition_by(partition_config) -%}
    {%- if partition_config is none -%}
      {% do return('') %}
    {%- elif partition_config.data_type | lower in ('date','timestamp','datetime') -%}
        partition by {{ partition_config.render() }}
    {%- elif partition_config.data_type | lower in ('int64') -%}
        {%- set range = partition_config.range -%}
        partition by range_bucket(
            {{ partition_config.field }},
            generate_array({{ range.start}}, {{ range.end }}, {{ range.interval }})
        )
    {%- endif -%}
{%- endmacro -%}

{% macro table_options(table_options_config) %}
    OPTIONS({% for opt_key, opt_val in table_options_config.options.items() %}
      {{ opt_key }}={{ opt_val }}{{ "," if not loop.last }}
    {% endfor %})
{%- endmacro -%}
