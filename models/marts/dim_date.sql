{{
  config(
    materialized = 'table',
    tags         = ['marts', 'dimensions', 'static'],
    post_hook    = [
      "GRANT SELECT ON {{ this }} TO ROLE REPORTER",
    ],
  )
}}

/*
  dim_date.sql
  Kimball Date Dimension — calendar + fiscal calendar.

  Spine: every calendar day from 1992-01-01 to 2030-12-31
  (TPC-H orders range 1992–1998; extended for future-proofing).

  Fiscal Year convention: starts October 1 (common enterprise standard).
  Override by updating get_fiscal_year / get_fiscal_quarter macros.
*/

with

date_spine as (

    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "cast('1992-01-01' as date)",
        end_date   = "cast('2030-12-31' as date)"
    ) }}

),

date_attributes as (

    select

        -- ── Surrogate key ────────────────────────────────────────────────────
        to_number(to_char(date_day, 'YYYYMMDD'))    as date_sk,

        -- ── Date ─────────────────────────────────────────────────────────────
        date_day                                    as full_date,

        -- ── Calendar hierarchy ───────────────────────────────────────────────
        year(date_day)                              as calendar_year,
        quarter(date_day)                           as calendar_quarter,
        month(date_day)                             as calendar_month,
        monthname(date_day)                         as month_name,
        left(monthname(date_day), 3)                as month_name_short,
        weekofyear(date_day)                        as week_of_year,
        dayofyear(date_day)                         as day_of_year,
        day(date_day)                               as day_of_month,
        dayofweek(date_day)                         as day_of_week,         -- 0=Sun, 6=Sat (Snowflake)
        dayname(date_day)                           as day_name,
        left(dayname(date_day), 3)                  as day_name_short,

        -- ── Calendar period labels ───────────────────────────────────────────
        to_char(date_day, 'YYYY-MM')                as year_month,
        concat('Q', quarter(date_day), '-', year(date_day))
                                                    as quarter_label,

        -- ── Fiscal calendar (Oct 1 start) ────────────────────────────────────
        {{ get_fiscal_year('date_day') }}           as fiscal_year,
        {{ get_fiscal_quarter('date_day') }}        as fiscal_quarter,

        -- Fiscal month (Oct=1, Nov=2, … Sep=12)
        case
            when month(date_day) >= 10
                then month(date_day) - 9
            else month(date_day) + 3
        end                                         as fiscal_month,

        concat('FY', {{ get_fiscal_year('date_day') }},
               '-Q', {{ get_fiscal_quarter('date_day') }})
                                                    as fiscal_quarter_label,

        -- ── Weekend / business day flags ─────────────────────────────────────
        iff(dayofweek(date_day) in (0, 6), true, false)
                                                    as is_weekend,
        iff(dayofweek(date_day) in (0, 6), false, true)
                                                    as is_weekday,

        -- ── Relative date helpers (refreshed every dbt run) ──────────────────
        iff(date_day = current_date(),   true, false)   as is_today,
        iff(date_day = current_date()-1, true, false)   as is_yesterday,
        iff(date_day >= date_trunc('month', current_date())
            and date_day <= current_date(),  true, false) as is_current_month,
        iff(date_day >= date_trunc('year',  current_date())
            and date_day <= current_date(),  true, false) as is_current_year,

        -- ── First / last of period flags ─────────────────────────────────────
        iff(day(date_day) = 1, true, false)             as is_first_of_month,
        iff(date_day = last_day(date_day), true, false) as is_last_of_month,

        -- Days since epoch (useful for lag/lead calculations in Looker)
        datediff('day', '1970-01-01'::date, date_day)   as days_since_epoch

    from date_spine

)

select * from date_attributes
