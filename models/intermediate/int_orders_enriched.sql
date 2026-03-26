{{
  config(
    materialized = 'ephemeral',
    tags         = ['intermediate', 'orders'],
  )
}}

/*
  int_orders_enriched.sql
  Grain: one row per order LINE ITEM (not per order header).

  Joins:
    stg_orders      (order header attributes)
    stg_customers   (customer + geography)
    lineitem source (individual quantities, prices, discounts, ship dates)
    stg_products    (product hierarchy)

  Responsibilities:
    - Materialise the canonical order-line grain used by both fact_orders
      and downstream marts
    - Calculate revenue metrics at line-item level
    - Derive shipping performance (on-time vs. late)
    - Apply fiscal calendar mapping
*/

with

orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

lineitems as (
    select
        l_orderkey                          as order_key,
        l_partkey                           as product_key,
        l_suppkey                           as supplier_key,
        l_linenumber                        as line_number,
        l_quantity::float                   as quantity,
        l_extendedprice::float              as extended_price,
        l_discount::float                   as discount_rate,
        l_tax::float                        as tax_rate,
        l_returnflag                        as return_flag,
        l_linestatus                        as line_status,
        l_shipdate::date                    as ship_date,
        l_commitdate::date                  as commit_date,
        l_receiptdate::date                 as receipt_date,
        l_shipinstruct                      as ship_instructions,
        l_shipmode                          as ship_mode,
        l_comment                           as line_comment
    from {{ source('tpch', 'lineitem') }}
),

enriched as (

    select

        -- ── Surrogate key ────────────────────────────────────────────────────
        {{ generate_surrogate_key(['li.order_key', 'li.line_number']) }}
                                            as order_line_sk,

        -- ── Order header ─────────────────────────────────────────────────────
        o.order_key,
        o.order_date,
        o.order_status,
        o.order_status_desc,
        o.order_priority,
        o.order_priority_rank,
        o.clerk_name,

        -- ── Line item ────────────────────────────────────────────────────────
        li.line_number,
        li.quantity,
        li.extended_price,
        li.discount_rate,
        li.tax_rate,
        li.return_flag,
        li.line_status,
        li.ship_mode,
        li.ship_instructions,
        li.ship_date,
        li.commit_date,
        li.receipt_date,

        -- ── Revenue metrics ──────────────────────────────────────────────────
        round(li.extended_price * (1 - li.discount_rate), 2)
                                            as net_revenue,
        round(li.extended_price * (1 - li.discount_rate) * (1 + li.tax_rate), 2)
                                            as gross_revenue,
        round(li.extended_price * li.discount_rate, 2)
                                            as discount_amount,
        round(li.extended_price * (1 - li.discount_rate) * li.tax_rate, 2)
                                            as tax_amount,

        -- ── Unit economics ───────────────────────────────────────────────────
        {{ safe_divide('li.extended_price', 'li.quantity') }}
                                            as unit_price,
        {{ safe_divide('li.extended_price * (1 - li.discount_rate)', 'li.quantity') }}
                                            as net_unit_price,

        -- ── Shipping performance ─────────────────────────────────────────────
        datediff('day', o.order_date,  li.ship_date)    as days_to_ship,
        datediff('day', li.ship_date,  li.receipt_date) as days_in_transit,
        datediff('day', o.order_date,  li.receipt_date) as total_fulfillment_days,

        case
            when li.ship_date <= li.commit_date then 'On-Time'
            else 'Late'
        end                                 as delivery_status,

        datediff('day', li.commit_date, li.ship_date)
                                            as days_late,   -- negative = early

        -- ── Fiscal period ────────────────────────────────────────────────────
        {{ get_fiscal_year('o.order_date') }}   as fiscal_year,
        {{ get_fiscal_quarter('o.order_date') }} as fiscal_quarter,

        -- ── Customer attributes ──────────────────────────────────────────────
        c.customer_key,
        c.customer_name,
        c.market_segment,
        c.nation_name,
        c.region_name,
        c.account_tier,
        c.account_balance,

        -- ── Product attributes ───────────────────────────────────────────────
        p.product_key,
        p.product_name,
        p.brand,
        p.manufacturer,
        p.product_material,
        p.product_size_segment,
        p.product_finish,
        p.container_size,
        p.retail_price,
        p.estimated_margin_pct,

        -- ── Supplier ─────────────────────────────────────────────────────────
        li.supplier_key

    from lineitems  li
    inner join orders    o  on li.order_key   = o.order_key
    inner join customers c  on o.customer_key = c.customer_key
    left  join products  p  on li.product_key = p.product_key

)

select * from enriched
