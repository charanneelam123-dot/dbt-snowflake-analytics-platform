{% macro assert_row_count(model, min_rows=1, max_rows=None) %}

    {#-
        assert_row_count(model, min_rows, max_rows)

        Custom test macro: asserts that a model has at least `min_rows` rows,
        and optionally at most `max_rows` rows.

        Returns rows where the assertion fails (empty = pass, for dbt test convention).

        Usage in schema.yml:
            tests:
              - assert_row_count:
                  min_rows: 1000
              - assert_row_count:
                  min_rows: 100
                  max_rows: 10000000

        Or as a standalone test SQL file referencing this macro.
    -#}

    with

    row_count as (
        select count(*) as cnt from {{ model }}
    ),

    validation as (
        select
            cnt,
            {{ min_rows }} as expected_min,
            {{ max_rows if max_rows is not none else 'null' }} as expected_max,
            case
                when cnt < {{ min_rows }}
                    then 'FAIL: row count ' || cnt || ' below minimum ' || {{ min_rows }}
                {% if max_rows is not none %}
                when cnt > {{ max_rows }}
                    then 'FAIL: row count ' || cnt || ' above maximum ' || {{ max_rows }}
                {% endif %}
                else null
            end as failure_reason
        from row_count
    )

    select * from validation where failure_reason is not null

{% endmacro %}


{% macro get_column_values(table, column, max_records=50) %}

    {#-
        get_column_values(table, column, max_records)

        Utility macro: returns distinct values of a column as a Python list.
        Useful for generating dynamic IN lists or accepted_values tests.

        Usage:
            {% set statuses = get_column_values(ref('stg_orders'), 'order_status') %}
    -#}

    {%- if execute -%}
        {%- set query -%}
            select distinct {{ column }}
            from {{ table }}
            where {{ column }} is not null
            order by {{ column }}
            limit {{ max_records }}
        {%- endset -%}
        {%- set results = run_query(query) -%}
        {%- set values = results.columns[0].values() -%}
        {{ return(values) }}
    {%- else -%}
        {{ return([]) }}
    {%- endif -%}

{% endmacro %}
