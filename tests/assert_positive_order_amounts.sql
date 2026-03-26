-- assert_positive_order_amounts.sql
-- Custom singular test: verifies that no fact_orders rows have negative
-- revenue, quantity, or price.
-- Returns rows that FAIL the assertion (dbt convention: empty = pass).

select
    order_line_sk,
    order_key,
    line_number,
    net_revenue,
    gross_revenue,
    quantity,
    extended_price,
    discount_amount,

    -- Capture which assertion failed for debugging
    case
        when net_revenue       < 0 then 'net_revenue < 0'
        when gross_revenue     < 0 then 'gross_revenue < 0'
        when quantity          <= 0 then 'quantity <= 0'
        when extended_price    <= 0 then 'extended_price <= 0'
        when discount_amount   < 0 then 'discount_amount < 0'
        when tax_amount        < 0 then 'tax_amount < 0'
    end as failure_reason

from {{ ref('fact_orders') }}

where
    net_revenue     < 0
    or gross_revenue    < 0
    or quantity         <= 0
    or extended_price   <= 0
    or discount_amount  < 0
    or tax_amount       < 0
