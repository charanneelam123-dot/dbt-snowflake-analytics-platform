{{
  config(
    materialized = 'view',
    tags         = ['staging', 'products'],
  )
}}

/*
  stg_products.sql
  Source: Snowflake Sample Data — TPCH_SF1.PART + PARTSUPP + SUPPLIER
  Grain: one row per part (200k rows at SF1)

  Responsibilities:
    - Parse product type and container from the composite p_type / p_container columns
    - Derive product category hierarchy (brand → container → type)
    - Join to best-price supplier for enrichment
*/

with

parts as (

    select * from {{ source('tpch', 'part') }}

),

partsupp as (

    select * from {{ source('tpch', 'partsupp') }}

),

suppliers as (

    select * from {{ source('tpch', 'supplier') }}

),

-- Cheapest available supplier per part (used for cost reference in fact table)
best_supplier as (

    select
        ps_partkey                                  as part_key,
        ps_suppkey                                  as supplier_key,
        ps_availqty                                 as available_qty,
        ps_supplycost::float                        as supply_cost,
        row_number() over (
            partition by ps_partkey
            order by ps_supplycost asc
        )                                           as cost_rank
    from partsupp

),

product_base as (

    select

        -- Keys
        p.p_partkey                         as product_key,
        bs.supplier_key,

        -- Descriptors
        p.p_name                            as product_name,
        p.p_mfgr                            as manufacturer,
        p.p_brand                           as brand,

        -- Type parsing  (e.g. "ECONOMY ANODIZED STEEL")
        p.p_type                            as product_type_raw,
        split_part(p.p_type, ' ', 1)        as product_size_segment,  -- ECONOMY / STANDARD / etc.
        split_part(p.p_type, ' ', 2)        as product_finish,         -- ANODIZED / POLISHED / etc.
        split_part(p.p_type, ' ', 3)        as product_material,       -- STEEL / BRASS / etc.

        -- Container  (e.g. "SM BOX")
        p.p_container                       as container_raw,
        split_part(p.p_container, ' ', 1)   as container_size,         -- SM / MED / LG / JUMBO / WRAP
        split_part(p.p_container, ' ', 2)   as container_type,         -- BOX / BAG / PACK / CAN / DRUM / JAR / PKG / CASE

        -- Size / cost
        p.p_size::int                       as product_size_units,
        p.p_retailprice::float              as retail_price,
        bs.supply_cost,
        bs.available_qty,

        -- Derived margin estimate
        {{ safe_divide('p.p_retailprice::float - bs.supply_cost', 'p.p_retailprice::float') }}
                                            as estimated_margin_pct,

        p.p_comment                         as product_comment,

        -- Audit
        current_timestamp()                 as _loaded_at

    from parts        p
    left join best_supplier bs
        on  p.p_partkey  = bs.part_key
        and bs.cost_rank = 1

)

select * from product_base
