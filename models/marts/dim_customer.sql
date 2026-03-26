{{
  config(
    materialized = 'table',
    tags         = ['marts', 'dimensions', 'scd2', 'daily'],
    post_hook    = [
      "GRANT SELECT ON {{ this }} TO ROLE REPORTER",
    ],
  )
}}

/*
  dim_customer.sql
  Kimball SCD Type 2 — Customer Dimension

  Tracks slowly-changing attributes: account_tier, market_segment,
  nation_name, region_name, account_balance.

  SCD2 columns:
    _dbt_scd_id      — row-level hash of all tracked attributes
    effective_from   — date this version became active
    effective_to     — date this version was superseded (9999-12-31 = current)
    is_current       — boolean flag for latest version
    customer_sk      — integer surrogate key (hash-based for stability)

  Implementation:
    Full table rebuild on each dbt run (materialized=table).
    A snapshot (snapshots/customer_snapshot.sql) captures daily deltas.
    This model reads from the snapshot to build the Type 2 history.

    If no snapshot has run yet, it builds from the current staging layer
    with a single "current" record per customer.
*/

with

-- Pull from snapshot if available, else fall back to staging
customer_history as (

    {% if execute %}
        {% set snapshot_exists = adapter.get_relation(
            database = target.database,
            schema   = target.schema ~ '_snapshots',
            identifier = 'customer_snapshot'
        ) %}
    {% else %}
        {% set snapshot_exists = false %}
    {% endif %}

    {% if snapshot_exists %}

        select
            c_custkey                       as customer_key,
            c_name                          as customer_name,
            c_address                       as customer_address,
            c_phone                         as customer_phone,
            c_mktsegment                    as market_segment,
            c_comment                       as customer_comment,
            n_name                          as nation_name,
            r_name                          as region_name,
            c_acctbal::float                as account_balance,
            dbt_scd_id                      as _dbt_scd_id,
            dbt_updated_at::date            as effective_from,
            coalesce(
                lead(dbt_updated_at::date) over (
                    partition by c_custkey order by dbt_updated_at
                ) - interval '1 day',
                '{{ var("scd2_far_future_date") }}'::date
            )                               as effective_to,
            dbt_valid_to is null            as is_current
        from {{ target.database }}.{{ target.schema }}_snapshots.customer_snapshot

    {% else %}

        -- No snapshot yet — build single-version history from current staging
        select
            customer_key,
            customer_name,
            customer_address,
            customer_phone,
            market_segment,
            customer_comment,
            nation_name,
            region_name,
            account_balance,
            md5(concat_ws('|',
                customer_key::string,
                customer_name,
                market_segment,
                nation_name,
                account_balance::string
            ))                              as _dbt_scd_id,
            '1992-01-01'::date              as effective_from,
            '{{ var("scd2_far_future_date") }}'::date as effective_to,
            true                            as is_current
        from {{ ref('stg_customers') }}

    {% endif %}

),

account_tier as (
    -- Recalculate tier per historical balance (balance may have changed)
    select
        *,
        case
            when account_balance < 0            then 'Negative'
            when account_balance between 0
                                 and 2500       then 'Bronze'
            when account_balance between 2500
                                 and 5000       then 'Silver'
            when account_balance between 5000
                                 and 7500       then 'Gold'
            else                                     'Platinum'
        end                                     as account_tier
    from customer_history
),

with_sk as (

    select

        -- ── Surrogate key (stable across SCD versions) ───────────────────────
        -- Use hash of natural key + effective_from so each version has unique SK
        {{ generate_surrogate_key(['customer_key', 'effective_from::string']) }}
                                            as customer_sk,

        -- ── Natural key ──────────────────────────────────────────────────────
        customer_key,

        -- ── Descriptors ──────────────────────────────────────────────────────
        customer_name,
        customer_address,
        customer_phone,
        market_segment,
        customer_comment,

        -- ── Geography ────────────────────────────────────────────────────────
        nation_name,
        region_name,

        -- ── Financials ───────────────────────────────────────────────────────
        account_balance,
        account_tier,

        -- ── SCD2 metadata ────────────────────────────────────────────────────
        _dbt_scd_id,
        effective_from,
        effective_to,
        is_current,

        -- ── Audit ────────────────────────────────────────────────────────────
        current_timestamp()                 as _dbt_loaded_at

    from account_tier

)

select * from with_sk
