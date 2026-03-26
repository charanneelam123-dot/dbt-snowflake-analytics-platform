-- assert_valid_order_status.sql
-- Business rule: only TLC-defined order statuses may appear in fact_orders.
-- Returns rows that FAIL (empty result = test passes).

with

valid_statuses as (
    select column1 as status
    from (values ('O'), ('F'), ('P')) as t
),

invalid_rows as (
    select
        fo.order_line_sk,
        fo.order_key,
        fo.order_date,
        fo.order_status,
        'Invalid order_status: ' || coalesce(fo.order_status, 'NULL') as failure_reason
    from {{ ref('fact_orders') }} fo
    left join valid_statuses vs on fo.order_status = vs.status
    where vs.status is null
)

select * from invalid_rows
