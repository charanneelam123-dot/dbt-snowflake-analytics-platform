# dbt-snowflake-analytics-platform

![dbt](https://img.shields.io/badge/dbt-1.7.4-FF694B?logo=dbt&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Analytics-29B5E8?logo=snowflake&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.8.1-017CEE?logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Kimball](https://img.shields.io/badge/Modeling-Kimball_DWH-blueviolet)
![CI](https://img.shields.io/github/actions/workflow/status/your-org/dbt-snowflake-analytics-platform/ci.yml?label=CI)

Production-grade **dbt + Snowflake analytics platform** using Kimball dimensional modeling. Powered by the TPC-H benchmark dataset (~1.5M orders, 150k customers, 200k products). Includes SCD Type 2, incremental fact tables, custom tests, fiscal calendar macros, and full CI/CD.

---

## Data Lineage

```
SOURCE LAYER (Snowflake Sample Data — TPCH_SF1)
════════════════════════════════════════════════
  ORDERS   CUSTOMER   PART   PARTSUPP   SUPPLIER   NATION   REGION
    │          │        │        │          │          │        │
    └────┬─────┘        └────┬───┘          └────┬─────┘        │
         │                  │                   │              │
         ▼                  ▼                   ▼              ▼
STAGING LAYER (views — rename, cast, validate)
════════════════════════════════════════════════
  stg_orders          stg_customers          stg_products
  ├── order_key       ├── customer_key       ├── product_key
  ├── customer_key    ├── market_segment     ├── brand / material
  ├── order_status    ├── account_tier       ├── retail_price
  ├── order_date      ├── nation_name        ├── supply_cost
  └── order_total_price └── region_name     └── estimated_margin_pct
         │                  │                   │
         └──────────────────┼───────────────────┘
                            │
                            ▼
INTERMEDIATE LAYER (ephemeral — compiles inline, no storage)
════════════════════════════════════════════════════════════
  int_orders_enriched   (order × lineitem × customer × product grain)
  ├── order_line_sk     (surrogate key: MD5(order_key, line_number))
  ├── net_revenue       = extended_price × (1 - discount)
  ├── gross_revenue     = net_revenue × (1 + tax)
  ├── delivery_status   = 'On-Time' if ship_date ≤ commit_date
  ├── fiscal_year / fiscal_quarter  (Oct 1 start macro)
  └── funnel_stage      (awareness → consideration → intent → purchase)
                            │
         ┌──────────────────┼──────────────────────┐
         │                  │                      │
         ▼                  ▼                      ▼
MARTS LAYER (Kimball Star Schema in Snowflake MARTS schema)
════════════════════════════════════════════════════════════

  DIMENSIONS                        FACTS
  ──────────                        ─────
  dim_date                          fact_orders  ◄─── INCREMENTAL MERGE
  ├── date_sk (YYYYMMDD int)        ├── order_line_sk (PK — MD5 SK)
  ├── full_date                     ├── customer_sk  ──► dim_customer
  ├── calendar_year/quarter/month   ├── product_sk   ──► dim_product
  ├── fiscal_year/quarter           ├── order_date_sk──► dim_date
  ├── is_weekend / is_weekday       ├── ship_date_sk ──► dim_date
  └── is_today / is_current_month   │
                                    │  ADDITIVE MEASURES
  dim_customer (SCD Type 2)         ├── quantity
  ├── customer_sk (versioned PK)    ├── net_revenue
  ├── customer_key (NK)             ├── gross_revenue
  ├── market_segment                ├── discount_amount
  ├── account_tier (tracked)        ├── tax_amount
  ├── nation_name / region_name     ├── days_to_ship
  ├── effective_from                ├── days_in_transit
  ├── effective_to                  └── total_fulfillment_days
  └── is_current
                                    DEGENERATE DIMENSIONS (inline)
  dim_product (SCD Type 1)          ├── order_status
  ├── product_sk                    ├── ship_mode
  ├── product_key (NK)              ├── delivery_status
  ├── brand / manufacturer          ├── fiscal_year / fiscal_quarter
  ├── product_material              └── market_segment / region_name
  ├── container_size
  └── retail_price / margin_pct

CUSTOM SINGULAR TESTS
════════════════════
  assert_positive_order_amounts    → no negative revenue/quantity rows
  assert_valid_order_status        → only O/F/P status codes in facts
  assert_scd2_no_gaps              → contiguous date ranges in dim_customer
  assert_fact_has_valid_dim_fks    → no orphaned FK values (-1 = unknown OK)
```

---

## Project Structure

```
dbt-snowflake-analytics-platform/
├── models/
│   ├── staging/
│   │   ├── stg_orders.sql           View: rename + cast + validate
│   │   ├── stg_customers.sql        View: customer + nation + region
│   │   ├── stg_products.sql         View: parts + best supplier
│   │   └── schema.yml               Source + model docs + tests
│   ├── intermediate/
│   │   ├── int_orders_enriched.sql  Ephemeral: order-line grain
│   │   └── schema.yml
│   └── marts/
│       ├── fact_orders.sql          Incremental MERGE fact table
│       ├── dim_customer.sql         SCD Type 2
│       ├── dim_date.sql             Calendar + fiscal spine
│       ├── dim_product.sql          SCD Type 1
│       └── schema.yml               All column docs + tests
├── tests/
│   ├── assert_positive_order_amounts.sql
│   ├── assert_valid_order_status.sql
│   ├── assert_scd2_no_gaps.sql
│   └── assert_fact_has_valid_dim_fks.sql
├── macros/
│   ├── generate_surrogate_key.sql   MD5-based SK macro
│   ├── safe_divide.sql              NULL-safe division
│   ├── fiscal_calendar.sql          get_fiscal_year/quarter/period_label
│   └── assert_row_count.sql         Reusable row-count test macro
├── dags/
│   └── dbt_pipeline_dag.py          Airflow DAG: deps→freshness→run→test→docs
├── dashboards/
│   └── analytics_overview.md        Power BI mockups + DAX measures
├── dbt_project.yml
├── profiles.yml.example
├── packages.yml                     dbt-utils, audit_helper, elementary
└── .github/workflows/ci.yml         SQL lint, compile, dbt build, docs publish
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Transformation | dbt-core 1.7.4 + dbt-snowflake |
| Warehouse | Snowflake (any edition) |
| Source data | TPC-H SF1 (Snowflake Sample Data) |
| Modeling | Kimball dimensional (Star Schema) |
| Orchestration | Apache Airflow 2.8 |
| Packages | dbt-utils, audit_helper, elementary |
| Visualization | Power BI / Looker (DirectQuery) |
| CI/CD | GitHub Actions (SQLFluff, dbt parse, dbt build, docs) |

---

## Setup

### 1. Clone & Install

```bash
git clone https://github.com/your-org/dbt-snowflake-analytics-platform.git
cd dbt-snowflake-analytics-platform
pip install dbt-core==1.7.4 dbt-snowflake==1.7.4
```

### 2. Configure credentials

```bash
cp profiles.yml.example ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your Snowflake account details
# Or set env vars: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, etc.
```

### 3. Install dbt packages

```bash
dbt deps
```

### 4. Verify Snowflake connection & source data

```bash
dbt debug
dbt source freshness
```

The source data lives at `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1` — available in all Snowflake accounts at no cost.

### 5. Run the full pipeline

```bash
# Full build: staging → intermediate (ephemeral) → dims → facts
dbt build

# Or layered:
dbt run  --select staging
dbt test --select staging
dbt run  --select marts.dim_date marts.dim_customer marts.dim_product
dbt run  --select marts.fact_orders
dbt test --select marts
```

### 6. Generate docs

```bash
dbt docs generate
dbt docs serve   # opens http://localhost:8080
```

---

## Macros Reference

```sql
-- Surrogate key (MD5 hash of pipe-delimited fields, NULL-safe)
{{ generate_surrogate_key(['order_key', 'line_number']) }}

-- NULL-safe division (returns NULL instead of divide-by-zero error)
{{ safe_divide('net_revenue', 'quantity') }}
{{ safe_divide('net_revenue', 'quantity', default=0) }}

-- Fiscal calendar (Oct 1 fiscal year start)
{{ get_fiscal_year('order_date') }}         -- returns INT
{{ get_fiscal_quarter('order_date') }}      -- returns 1-4
{{ get_fiscal_period_label('order_date') }} -- returns 'FY2024-Q1'
```

---

## CI/CD Pipeline

Every PR triggers:

| Job | What it does |
|---|---|
| `sql-lint` | SQLFluff on all models + tests (Snowflake dialect) |
| `dbt-compile` | `dbt parse` + `dbt compile` — validates all Jinja/SQL syntax without a DB connection |
| Schema completeness | Asserts all models + columns have descriptions in schema.yml |
| `schema-drift` | Warns if SQL changed without a schema.yml update |
| `dbt-build` | Full `dbt build` + `dbt test` on isolated Snowflake CI schema (requires `snowflake-ci` PR label) |
| `publish-docs` | Deploys `dbt docs generate` output to GitHub Pages (main branch only) |

---

## dbt Tests Coverage

| Layer | Test type | Count |
|---|---|---|
| Staging | `not_null`, `unique`, `accepted_values`, `relationships` | 30+ |
| Intermediate | `not_null`, `unique`, `expression_is_true` | 12 |
| Marts — dims | `not_null`, `unique`, `accepted_values`, `expression_is_true` | 25+ |
| Marts — facts | `not_null`, `unique`, `expression_is_true`, `accepted_values` | 20+ |
| Custom singular | Business logic tests | 4 |
| **Total** | | **90+** |

---

## Contributing

1. Branch: `git checkout -b feat/my-model`
2. Add model SQL + schema.yml description for every column
3. Add at least `not_null` + `unique` on PKs, `accepted_values` on categoricals
4. Run `dbt build --select +my_model+` locally
5. Open PR → CI runs automatically

