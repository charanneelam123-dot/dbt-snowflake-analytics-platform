{{
  config(
    materialized = 'table',
    tags         = ['marts', 'dimensions', 'daily'],
    post_hook    = [
      "GRANT SELECT ON {{ this }} TO ROLE REPORTER",
    ],
  )
}}

/*
  dim_product.sql
  Kimball Product Dimension (SCD Type 1 — overwrite on change).

  Source: stg_products
  Grain: one row per product (part)
  Note: retail_price and supply_cost are slowly changing — tracked as
        Type 1 (last value wins). Promote to Type 2 if price history needed.
*/

with

products as (
    select * from {{ ref('stg_products') }}
),

with_sk as (

    select

        -- ── Surrogate key ────────────────────────────────────────────────────
        {{ generate_surrogate_key(['product_key']) }}   as product_sk,

        -- ── Natural key ──────────────────────────────────────────────────────
        product_key,

        -- ── Descriptors ──────────────────────────────────────────────────────
        product_name,
        manufacturer,
        brand,

        -- ── Type hierarchy ───────────────────────────────────────────────────
        product_type_raw,
        product_size_segment,
        product_finish,
        product_material,

        -- ── Container ────────────────────────────────────────────────────────
        container_raw,
        container_size,
        container_type,

        -- ── Size & pricing ───────────────────────────────────────────────────
        product_size_units,
        retail_price,
        supply_cost,
        available_qty,
        estimated_margin_pct,

        -- ── Audit ────────────────────────────────────────────────────────────
        current_timestamp()     as _dbt_loaded_at

    from products

)

select * from with_sk
