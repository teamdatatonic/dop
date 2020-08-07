{% import 'adapter.sql' as adapter %}

{% macro upsert(query, relation_helper, options) %}
    {%- set relation = options['relation'] -%}
    {%- set tmp_relation = options['tmp_relation'] -%}
    {%- set relation_exists = relation_helper.check_relation_exists(relation) -%}
    {%- set tmp_relation_exists = relation_helper.check_relation_exists(tmp_relation) -%}
    {%- set columns_of_relation = relation_helper.get_columns_of_relation(relation) -%}
    {%- set partition_config = options['partition_config'] -%}
    {%- set full_refresh = options['full_refresh'] -%}
    {%- set incremental_time_partitioned = partition_config.data_type | lower in ('date','timestamp','datetime') -%}

    {# -- Only run the merge query if tmp relation is produced and we already have an existing relation. This is not applicable for full refresh #}
    {%- if tmp_relation_exists and relation_exists and not full_refresh -%}
        {%- if not tmp_relation_exists -%}
            {{ raise('`{{ tmp_relation }}` must exist before an upsert can be done') }}
        {%- endif -%}

        {%- if not relation_helper.check_if_schemas_match(tmp_relation, relation) -%}
            {{ raise('Schema is backwards incompatible, when making schema changes, a full refresh is required') }}
        {%- endif -%}

        {# -- For time based partition only, workout the partitions to be replaced #}
        {%- if incremental_time_partitioned -%}
            declare dbt_partitions_for_replacement array<date>;
            set (dbt_partitions_for_replacement) = (
                  select as struct
                      array_agg(distinct {{ partition_config.render() }})
                  from {{ tmp_relation }}
              );
        {%- endif -%}

        merge into {{ relation }} as target_relation
            using (
            select * from {{ tmp_relation }}
          ) as tmp_relation
            on FALSE

        when not matched by source
            {%- if incremental_time_partitioned -%}
            {{ space }} and {{ partition_config.render(alias='target_relation') }} in unnest(dbt_partitions_for_replacement)
            {%- endif -%}
            {{ space }} then delete

        when not matched then insert
        ({% for col in columns_of_relation %}`{{ col }}`{{ "," if not loop.last }}{% endfor %})
        VALUES ({% for col in columns_of_relation %}`{{ col }}`{{ "," if not loop.last }}{% endfor %});

    {%- endif -%}
    {# -- Always try to drop the tmp table #}
    drop table if exists {{ tmp_relation }};
{% endmacro %}
