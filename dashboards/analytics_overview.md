# Analytics Platform — Dashboard Specifications

> This document describes the Power BI / Looker dashboards powered by the Gold marts.
> All dashboards connect directly to Snowflake marts schema via DirectQuery or Import mode.

---

## Dashboard 1 — Executive Revenue Overview

**Target audience:** C-suite, VP Sales
**Refresh:** Daily at 06:00 UTC
**Snowflake tables:** `fact_orders`, `dim_date`, `dim_customer`

```
┌────────────────────────────────────────────────────────────────────────────┐
│  EXECUTIVE REVENUE OVERVIEW                         Last updated: 2024-01-15│
├────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  KPI Cards                                                                  │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐      │
│  │ Total Revenue│ │  # Orders    │ │  Avg Order   │ │  On-Time %   │      │
│  │  $217.3M    │ │   1.5M       │ │   $144.72    │ │    87.4%     │      │
│  │  ▲ 12.3% YoY│ │  ▲ 8.1% YoY │ │  ▲ 3.9% YoY │ │  ▼ 1.2pp    │      │
│  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘      │
│                                                                             │
│  Revenue by Month (Bar + Line combo)                                        │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  $25M ┤                              ██  ██                    │        │
│  │  $20M ┤          ██  ██  ██  ██  ██  ██  ██  ──────────── MoM│        │
│  │  $15M ┤  ██  ██  ██  ██  ██  ██  ██  ██  ██                  │        │
│  │  $10M ┤  ██  ██  ██  ██  ██  ██  ██  ██  ██                  │        │
│  │       └──Jan─Feb─Mar─Apr─May─Jun─Jul─Aug─Sep─Oct─Nov─Dec      │        │
│  └────────────────────────────────────────────────────────────────┘        │
│                                                                             │
│  Revenue by Market Segment (Donut)   │  Revenue by Region (Map)            │
│  ┌──────────────────────┐            │  ┌──────────────────────────┐       │
│  │    AUTOMOBILE 23%    │            │  │  AMERICA     $82.1M  38% │       │
│  │    BUILDING   21%    │            │  │  EUROPE      $67.4M  31% │       │
│  │    FURNITURE  20%    │            │  │  ASIA        $43.8M  20% │       │
│  │    HOUSEHOLD  19%    │            │  │  MIDDLE EAST $15.2M   7% │       │
│  │    MACHINERY  17%    │            │  │  AFRICA       $8.8M   4% │       │
│  └──────────────────────┘            │  └──────────────────────────┘       │
│                                                                             │
│  Slicers: Fiscal Year | Quarter | Region | Market Segment                  │
└────────────────────────────────────────────────────────────────────────────┘

DAX Measures (Power BI):
  Total Revenue    = SUM(fact_orders[net_revenue])
  YoY Revenue %   = DIVIDE([Total Revenue] - [LY Revenue], [LY Revenue])
  LY Revenue       = CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(dim_date[full_date]))
  On-Time Rate     = DIVIDE(COUNTROWS(FILTER(fact_orders, [is_late_delivery]=FALSE())),
                             COUNTROWS(fact_orders))
```

---

## Dashboard 2 — Order Operations & Shipping Performance

**Target audience:** Operations, Supply Chain
**Refresh:** Daily
**Snowflake tables:** `fact_orders`, `dim_date`, `dim_product`

```
┌────────────────────────────────────────────────────────────────────────────┐
│  ORDER OPERATIONS                                                           │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Shipping KPIs                                                              │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐      │
│  │ Avg Days     │ │ Avg Transit  │ │ Late Orders  │ │ Return Rate  │      │
│  │ to Ship      │ │ Days         │ │              │ │              │      │
│  │    2.3 days  │ │   5.1 days   │ │    12.6%     │ │    4.8%      │      │
│  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘      │
│                                                                             │
│  On-Time Delivery by Ship Mode (Stacked Bar)                                │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  AIR       ████████████████████████████████░░░░░░░  94% OT    │        │
│  │  MAIL      ██████████████████████████░░░░░░░░░░░░░  79% OT    │        │
│  │  SHIP      ████████████████████░░░░░░░░░░░░░░░░░░░  71% OT    │        │
│  │  RAIL      ███████████████████████████░░░░░░░░░░░░  83% OT    │        │
│  │  TRUCK     ███████████████████████████████░░░░░░░░  88% OT    │        │
│  │            ████ On-Time  ░░░░ Late                            │        │
│  └────────────────────────────────────────────────────────────────┘        │
│                                                                             │
│  Fulfillment Days Distribution (Histogram)                                  │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  Count  ▄▄                                                     │        │
│  │         ████                                                   │        │
│  │         ██████  ████                                           │        │
│  │         ██████████████  ██                                     │        │
│  │         ██████████████████████  ████                          │        │
│  │        ─1─2─3─4─5─6─7─8─9─10─11─12─13─14+ days              │        │
│  └────────────────────────────────────────────────────────────────┘        │
│                                                                             │
│  Late Orders by Product Material (Table)                                    │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  Material      Total   Late   Late%   Avg Days Late            │        │
│  │  STEEL          45,231  6,123  13.5%      2.3                  │        │
│  │  BRASS          38,102  4,891  12.8%      1.9                  │        │
│  │  COPPER         29,847  4,121  13.8%      2.7                  │        │
│  │  NICKEL         25,103  3,012  12.0%      1.8                  │        │
│  └────────────────────────────────────────────────────────────────┘        │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Dashboard 3 — Customer Segmentation & Cohort Analysis

**Target audience:** Marketing, Customer Success
**Refresh:** Daily
**Snowflake tables:** `fact_orders`, `dim_customer`, `dim_date`

```
┌────────────────────────────────────────────────────────────────────────────┐
│  CUSTOMER SEGMENTATION                                                      │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Customer Account Tier Revenue (Stacked Area)                               │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  $25M ┤                                        ████ Platinum  │        │
│  │  $20M ┤                                   ████ ████ Gold      │        │
│  │  $15M ┤                              ████ ████ ████ Silver    │        │
│  │  $10M ┤                         ████ ████ ████ ████ Bronze    │        │
│  │   $5M ┤ ████ ████ ████ ████ ████ ████ ████ ████ ████ Negative│        │
│  │       └─Q1──Q2──Q3──Q4──Q1──Q2──Q3──Q4──Q1──Q2──Q3──Q4──     │        │
│  └────────────────────────────────────────────────────────────────┘        │
│                                                                             │
│  Revenue per Customer by Segment (Box Plot)                                 │
│  Shows median, IQR, outliers per market segment                            │
│                                                                             │
│  Top 20 Customers (Table — sorted by net_revenue desc)                     │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  Rank  Customer Name    Tier      Segment    Revenue   Orders  │        │
│  │  1     Customer#...     Platinum  AUTO       $142K     312     │        │
│  │  2     Customer#...     Gold      BUILDING   $138K     287     │        │
│  │  ...                                                           │        │
│  └────────────────────────────────────────────────────────────────┘        │
│                                                                             │
│  SCD2 Tier Migration Sankey (shows how customers move between tiers)        │
│  Bronze → Silver: 1,247 customers                                           │
│  Silver → Gold:     891 customers                                           │
│  Gold → Platinum:   234 customers                                           │
│  Gold → Silver:     156 customers  (downgrade)                              │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Power BI Connection Setup

```
Server:   <account>.snowflakecomputing.com
Database: ANALYTICS
Schema:   MARTS
Role:     REPORTER
Warehouse: ANALYTICS_WH_XS
Authentication: Username + Password (or SSO via AAD)
DirectQuery mode: recommended for > 10M rows
```

## Key DAX Measures Reference

```dax
-- Revenue
Total Net Revenue = SUM(fact_orders[net_revenue])
Avg Order Value   = AVERAGEX(VALUES(fact_orders[order_key]),
                     CALCULATE(SUM(fact_orders[net_revenue])))

-- Time intelligence
YoY Revenue    = DIVIDE([Total Net Revenue] - [LY Net Revenue], [LY Net Revenue])
MTD Revenue    = CALCULATE([Total Net Revenue], DATESMTD(dim_date[full_date]))
QTD Revenue    = CALCULATE([Total Net Revenue], DATESQTD(dim_date[full_date]))

-- Operations
On-Time Rate   = DIVIDE(
                   CALCULATE(COUNTROWS(fact_orders), fact_orders[is_late_delivery]=FALSE()),
                   COUNTROWS(fact_orders))
Avg Days Late  = AVERAGEX(FILTER(fact_orders, fact_orders[days_late]>0), fact_orders[days_late])
Return Rate    = DIVIDE(
                   CALCULATE(COUNTROWS(fact_orders), fact_orders[is_returned]=TRUE()),
                   COUNTROWS(fact_orders))
```
