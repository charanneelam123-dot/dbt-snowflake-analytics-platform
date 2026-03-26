-- assert_scd2_no_gaps.sql
-- SCD Type 2 integrity check: for each customer_key, the effective date
-- ranges must be contiguous with no gaps and no overlaps.
-- Returns customer_key rows where gaps/overlaps exist.

with

customer_versions as (

    select
        customer_key,
        customer_sk,
        effective_from,
        effective_to,
        is_current,
        -- Next version's effective_from (should equal this version's effective_to + 1 day)
        lead(effective_from) over (
            partition by customer_key
            order by effective_from
        ) as next_version_from
    from {{ ref('dim_customer') }}

),

gap_check as (

    select
        customer_key,
        customer_sk,
        effective_from,
        effective_to,
        next_version_from,

        -- Gap: this version's effective_to is not contiguous with next version
        case
            when next_version_from is not null
             and effective_to <> next_version_from
            then 'GAP or OVERLAP between versions'
            else null
        end as gap_reason,

        -- Overlap: effective_to must be > effective_from
        case
            when effective_to < effective_from
            then 'effective_to before effective_from'
            else null
        end as range_reason

    from customer_versions

),

failures as (

    select
        customer_key,
        customer_sk,
        effective_from,
        effective_to,
        next_version_from,
        coalesce(gap_reason, range_reason) as failure_reason
    from gap_check
    where gap_reason is not null
       or range_reason is not null

)

select * from failures
