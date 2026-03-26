{{
  config(
    materialized = 'view',
    tags         = ['staging', 'orders'],
  )
}}

/*
  stg_orders.sql
  Source: Snowflake Sample Data — TPCH_SF1.ORDERS
  Grain: one row per order (1.5M rows at SF1)

  Responsibilities:
    - Rename columns to snake_case business vocabulary
    - Cast all columns to canonical types
    - Derive simple flags / categorizations
    - NO joins — pure source-layer cleaning
*/

with

source as (

    select * from {{ source('tpch', 'orders') }}

),

renamed as (

    select

        -- Keys
        o_orderkey              as order_key,
        o_custkey               as customer_key,

        -- Descriptors
        o_orderstatus           as order_status,
        o_orderpriority         as order_priority,
        o_clerk                 as clerk_name,
        o_shippriority          as ship_priority,
        o_comment               as order_comment,

        -- Amounts
        o_totalprice::float     as order_total_price,

        -- Dates
        o_orderdate::date       as order_date,

        -- Derived flags
        case o_orderstatus
            when 'O' then 'Open'
            when 'F' then 'Fulfilled'
            when 'P' then 'In Progress'
            else 'Unknown'
        end                     as order_status_desc,

        case
            when o_orderpriority = '1-URGENT'        then 1
            when o_orderpriority = '2-HIGH'          then 2
            when o_orderpriority = '3-MEDIUM'        then 3
            when o_orderpriority = '4-NOT SPECIFIED' then 4
            when o_orderpriority = '5-LOW'           then 5
            else 99
        end                     as order_priority_rank,

        -- Audit
        current_timestamp()     as _loaded_at

    from source

),

validated as (

    select *
    from renamed
    where
        order_key       is not null
        and customer_key is not null
        and order_date   is not null
        and order_total_price >= 0

)

select * from validated
