{{
  config(
    materialized          = 'incremental',
    incremental_strategy  = 'merge',
    unique_key            = 'order_line_sk',
    cluster_by            = ['order_date', 'market_segment'],
    tags                  = ['marts', 'facts', 'daily'],
    on_schema_change      = 'append_new_columns',
    post_hook             = [
      "GRANT SELECT ON {{ this }} TO ROLE REPORTER",
    ],
  )
}}

/*
  fact_orders.sql
  Kimball Fact Table — Transaction Fact at ORDER LINE ITEM grain.

  Grain:     1 row per order × line number
  Keys:      order_line_sk (SK), order_key, customer_sk, product_sk, date_sk
  Measures:  quantity, extended_price, net_revenue, gross_revenue,
             discount_amount, tax_amount, days_to_ship, days_in_transit,
             total_fulfillment_days, days_late

  Incremental Strategy:
    MERGE on order_line_sk. Look back {{ var('incremental_lookback_days') }}
    days to catch late-arriving source updates (e.g. order status changes).

  Degenerate Dimensions (stored inline):
    order_status, order_priority, ship_mode, delivery_status,
    return_flag, fiscal_year, fiscal_quarter
*/

with

source as (

    select * from {{ ref('int_orders_enriched') }}

    {% if is_incremental() %}
        -- Incremental look-back window — reprocess recent window to handle
        -- late data and source corrections
        where order_date >= dateadd(
            day,
            -{{ var('incremental_lookback_days') }},
            (select max(order_date) from {{ this }})
        )
    {% endif %}

),

dim_customer as (
    -- Pull the current (is_current = true) dimension row for FK resolution
    select
        customer_key,
        customer_sk,
        account_tier,
        market_segment,
        nation_name,
        region_name
    from {{ ref('dim_customer') }}
    where is_current = true
),

dim_date as (
    select
        full_date,
        date_sk
    from {{ ref('dim_date') }}
),

dim_product as (
    select
        product_key,
        product_sk
    from {{ ref('dim_product') }}
),

final as (

    select

        -- ── Surrogate key ────────────────────────────────────────────────────
        s.order_line_sk,

        -- ── Foreign keys (Kimball FK pattern) ───────────────────────────────
        coalesce(dc.customer_sk, -1)        as customer_sk,     -- -1 = unknown
        coalesce(dp.product_sk,  -1)        as product_sk,
        coalesce(dd.date_sk,     -1)        as order_date_sk,
        coalesce(sd.date_sk,     -1)        as ship_date_sk,

        -- ── Degenerate dimensions (no separate dim table needed) ─────────────
        s.order_key,
        s.line_number,
        s.order_status,
        s.order_status_desc,
        s.order_priority,
        s.order_priority_rank,
        s.ship_mode,
        s.ship_instructions,
        s.return_flag,
        s.line_status,
        s.delivery_status,
        s.clerk_name,

        -- ── Conformed dimensions (denormalised for query performance) ─────────
        s.market_segment,
        s.nation_name,
        s.region_name,
        s.brand,
        s.product_material,
        s.product_size_segment,
        s.container_size,

        -- ── Fiscal calendar ─────────────────────────────────────────────────
        s.fiscal_year,
        s.fiscal_quarter,

        -- ── Additive measures ────────────────────────────────────────────────
        s.quantity,
        s.extended_price,
        s.discount_amount,
        s.net_revenue,
        s.gross_revenue,
        s.tax_amount,
        s.unit_price,
        s.net_unit_price,

        -- ── Semi-additive / non-additive measures ────────────────────────────
        s.discount_rate,                    -- % — average across lines, don't SUM
        s.tax_rate,
        s.estimated_margin_pct,             -- % — weighted average, don't SUM

        -- ── Latency / shipping measures ──────────────────────────────────────
        s.days_to_ship,
        s.days_in_transit,
        s.total_fulfillment_days,
        s.days_late,

        -- ── Derived flags (for slice-and-dice without a dimension lookup) ────
        (s.delivery_status = 'Late')::boolean       as is_late_delivery,
        (s.return_flag     = 'R')::boolean          as is_returned,
        (s.discount_rate   > 0)::boolean            as has_discount,
        (s.quantity        > 1)::boolean            as is_multi_unit,

        -- ── Audit ────────────────────────────────────────────────────────────
        s.order_date,
        s.ship_date,
        s.commit_date,
        s.receipt_date,
        current_timestamp()                         as _dbt_loaded_at

    from source  s
    left join dim_customer dc on s.customer_key = dc.customer_key
    left join dim_date     dd on s.order_date   = dd.full_date
    left join dim_date     sd on s.ship_date    = sd.full_date
    left join dim_product  dp on s.product_key  = dp.product_key

)

select * from final
