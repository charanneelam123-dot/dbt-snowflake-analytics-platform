{% macro generate_surrogate_key(field_list) %}

    {#-
        generate_surrogate_key(field_list)

        Generates a deterministic MD5 surrogate key by concatenating the
        given list of column expressions with a pipe delimiter.

        Handles NULL values by coalescing to the string '@@NULL@@' before
        hashing, ensuring that NULL combinations produce a consistent key
        rather than a NULL result.

        Usage:
            {{ generate_surrogate_key(['order_key', 'line_number']) }}
            {{ generate_surrogate_key(['customer_key', 'effective_from::string']) }}

        Snowflake equivalent: md5(concat_ws('|', col1, col2, ...))
        We coerce to string explicitly for non-string types.
    -#}

    {%- if field_list | length == 0 -%}
        {{ exceptions.raise_compiler_error(
            "generate_surrogate_key requires at least one field."
        ) }}
    {%- endif -%}

    md5(
        concat_ws(
            '|',
            {%- for field in field_list %}
                coalesce(cast({{ field }} as varchar), '@@NULL@@')
                {%- if not loop.last %},{% endif %}
            {%- endfor %}
        )
    )

{% endmacro %}
