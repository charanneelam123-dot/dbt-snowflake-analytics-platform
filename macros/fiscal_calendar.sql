{% macro get_fiscal_year(date_col) %}

    {#-
        get_fiscal_year(date_col)

        Returns the fiscal year for a given date column.
        Convention: fiscal year starts October 1.
        Example: 2023-10-01 → FY2024,  2024-09-30 → FY2024.

        Args:
            date_col: SQL date expression (column name or expression)

        Returns: INTEGER fiscal year
    -#}

    case
        when month({{ date_col }}) >= 10
            then year({{ date_col }}) + 1
        else
            year({{ date_col }})
    end

{% endmacro %}


{% macro get_fiscal_quarter(date_col) %}

    {#-
        get_fiscal_quarter(date_col)

        Returns the fiscal quarter [1..4] for a given date column.
        Fiscal quarters (Oct 1 start):
            Q1: Oct, Nov, Dec
            Q2: Jan, Feb, Mar
            Q3: Apr, May, Jun
            Q4: Jul, Aug, Sep

        Args:
            date_col: SQL date expression

        Returns: INTEGER 1–4
    -#}

    case
        when month({{ date_col }}) in (10, 11, 12) then 1
        when month({{ date_col }}) in ( 1,  2,  3) then 2
        when month({{ date_col }}) in ( 4,  5,  6) then 3
        when month({{ date_col }}) in ( 7,  8,  9) then 4
    end

{% endmacro %}


{% macro get_fiscal_period_label(date_col) %}

    {#-
        Returns a human-readable fiscal period label, e.g. "FY2024-Q1".
    -#}

    concat(
        'FY', {{ get_fiscal_year(date_col) }},
        '-Q', {{ get_fiscal_quarter(date_col) }}
    )

{% endmacro %}
