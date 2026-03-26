-- assert_fact_has_valid_dim_fks.sql
-- Referential integrity check: every non-unknown FK in fact_orders
-- must resolve to a row in the corresponding dimension.
-- Returns orphaned fact rows (empty = pass).

with

orphaned_customers as (
    select
        fo.order_line_sk,
        fo.customer_sk,
        'dim_customer' as failed_dimension,
        'customer_sk ' || fo.customer_sk || ' not found in dim_customer' as reason
    from {{ ref('fact_orders') }} fo
    where fo.customer_sk <> -1    -- -1 = intentional unknown member
      and not exists (
          select 1 from {{ ref('dim_customer') }} dc
          where dc.customer_sk = fo.customer_sk
      )
),

orphaned_products as (
    select
        fo.order_line_sk,
        fo.product_sk,
        'dim_product' as failed_dimension,
        'product_sk ' || fo.product_sk || ' not found in dim_product' as reason
    from {{ ref('fact_orders') }} fo
    where fo.product_sk <> -1
      and not exists (
          select 1 from {{ ref('dim_product') }} dp
          where dp.product_sk = fo.product_sk
      )
),

orphaned_dates as (
    select
        fo.order_line_sk,
        fo.order_date_sk,
        'dim_date' as failed_dimension,
        'order_date_sk ' || fo.order_date_sk || ' not found in dim_date' as reason
    from {{ ref('fact_orders') }} fo
    where fo.order_date_sk <> -1
      and not exists (
          select 1 from {{ ref('dim_date') }} dd
          where dd.date_sk = fo.order_date_sk
      )
)

select * from orphaned_customers
union all
select * from orphaned_products
union all
select * from orphaned_dates
