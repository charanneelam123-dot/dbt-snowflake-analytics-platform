{{
  config(
    materialized = 'view',
    tags         = ['staging', 'customers'],
  )
}}

/*
  stg_customers.sql
  Source: Snowflake Sample Data — TPCH_SF1.CUSTOMER
  Grain: one row per customer (150k rows at SF1)

  Responsibilities:
    - Rename to business vocabulary
    - Parse nation / market segment for downstream SCD Type 2
    - Classify customers by account balance tier
*/

with

source as (

    select * from {{ source('tpch', 'customer') }}

),

nations as (

    select * from {{ source('tpch', 'nation') }}

),

regions as (

    select * from {{ source('tpch', 'region') }}

),

enriched as (

    select

        -- Keys
        c.c_custkey                     as customer_key,
        c.c_nationkey                   as nation_key,
        n.n_regionkey                   as region_key,

        -- Descriptors
        c.c_name                        as customer_name,
        c.c_address                     as customer_address,
        c.c_phone                       as customer_phone,
        c.c_mktsegment                  as market_segment,
        c.c_comment                     as customer_comment,

        -- Geography
        n.n_name                        as nation_name,
        r.r_name                        as region_name,

        -- Financials
        c.c_acctbal::float              as account_balance,

        -- Derived — account balance tier (Kimball conformed attribute)
        case
            when c.c_acctbal < 0           then 'Negative'
            when c.c_acctbal between 0
                               and 2500    then 'Bronze'
            when c.c_acctbal between 2500
                               and 5000   then 'Silver'
            when c.c_acctbal between 5000
                               and 7500   then 'Gold'
            else                               'Platinum'
        end                             as account_tier,

        -- Audit
        current_timestamp()             as _loaded_at

    from source            c
    left join nations      n on c.c_nationkey = n.n_nationkey
    left join regions      r on n.n_regionkey = r.r_regionkey

)

select * from enriched
