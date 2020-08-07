{% import 'adapter.sql' as adapter %}

{% macro create_or_replace(query, relation_helper, options) %}
    {%- set relation = options['relation'] -%}
    {%- set tmp_relation = options['tmp_relation'] -%}
    {%- set relation_exists = relation_helper.check_relation_exists(relation) -%}
    {%- set table_options_config = options['table_options_config'] -%}
    {%- set partition_config = options['partition_config'] -%}
    {%- set full_refresh = options['full_refresh'] -%}

    {# -- Always try to drop the tmp table #}
    drop table if exists {{ tmp_relation }};

    {# -- Drop table first if partition definition is different and it's a full refresh #}
    {%- if full_refresh and relation_helper.has_same_partition_definition(partition_config, relation)  -%}
        drop table if exists {{ relation }};
    {%- endif -%}

    {# -- Create / Replace table #}
    create or replace table
    {%- if relation_exists and not full_refresh -%}
        {{ tmp_relation }}
    {%- else -%}
        {{ relation }}
    {%- endif -%}

    {# -- Partition Block #}
    {%- if partition_config is not none -%}
        {{ space }} {{ adapter.partition_by(partition_config) }}
    {%- endif -%}

    {# -- Table Options Block #}
    {%- if table_options_config is not none -%}
        {{ space }} {{ adapter.table_options(table_options_config) }}
    {%- endif -%}

    {# -- Main query block #}
    AS (
        {{ query }}
    );
{% endmacro %}
