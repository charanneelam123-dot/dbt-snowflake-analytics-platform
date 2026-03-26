{% macro safe_divide(numerator, denominator, default=None) %}

    {#-
        safe_divide(numerator, denominator, default=None)

        Division that returns NULL (or a specified default) instead of
        raising a division-by-zero error when the denominator is 0 or NULL.

        Args:
            numerator:   SQL expression for the dividend
            denominator: SQL expression for the divisor
            default:     Value to return when denominator is 0 or NULL.
                         Defaults to NULL.

        Usage:
            {{ safe_divide('total_revenue', 'order_count') }}
            {{ safe_divide('net_revenue', 'retail_price', default=0) }}
            {{ safe_divide('tip_amount', 'fare_amount') }}
    -#}

    case
        when ({{ denominator }}) = 0 or ({{ denominator }}) is null
            then {{ default if default is not none else 'null' }}
        else ({{ numerator }}) / ({{ denominator }})
    end

{% endmacro %}
