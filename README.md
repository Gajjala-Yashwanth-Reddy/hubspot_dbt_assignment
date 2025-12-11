# Hubspot Rental Analytics Take-Home Assessment

## Business Case

A rental property dataset has been provided with information about listings, calendar availability, amenities, and reviews. The goal of this project is to build a **dbt Core** project that enables business stakeholders to derive insights from these data sources and answer analytical questions such as:

- How much revenue do listings generate month-by-month, segmented by key amenities?
- How have listing prices changed over time in each neighborhood?
- What is the maximum possible stay duration per listing, and how does this vary with specific amenity flags?

To support these outcomes, this project standardizes the raw datasets, enriches them with temporal logic, and produces a curated mart for self-serve analytics.

---

## Tools & Environment

- **dbt Core**
- **Databricks Unity Catalog**
- **SQL** (Databricks SQL)
- **Version Control:** GitHub
- **SQLfmt**

---

## dbt Project Structure
<pre>
rental_analytics
├── analyses
│   ├── amenity_revenue.sql
│   ├── maximum_stay_lockbox_firstaid.sql
│   ├── maximum_stay_possible.sql
│   └── neighbourhood_pricing_change.sql
│
├── macros
│   └── get_custom_schema.sql
│
├── models
│   ├── intermediate
│   │   ├── _int_daily__models.yml
│   │   ├── int_daily_amenities.sql
│   │   ├── int_daily_reviews.sql
│   │   └── int_calendar_enriched.sql
│   │
│   ├── marts
│   │   ├── rentals
│   │   │   ├── _rental_listing__models.yml
│   │   │   └── rental__listing_daily_metrics.sql
│   │
│   └── staging
│       ├── _rental_company__models.yml
│       ├── _rental_company__sources.yml
│       ├── stg__listings.sql
│       ├── stg__calendar.sql
│       ├── stg__generated_reviews.sql
│       └── stg__amenities_changelog.sql
│
├── seeds
│
├── snapshots
│
└── tests
</pre>

---

### Layer Descriptions

#### 1. Source

Raw tables representing the CSVs loaded into Unity Catalog:

- `listings`
- `calendar`
- `generated_reviews`
- `amenities_changelog`

These are loaded into the catalog as `rental_analytics.source`.

#### 2. Staging

Light transformations and normalizations such as renaming fields, casting types, and standardizing columns.

Examples:
- `stg__listings`
- `stg__calendar`
- `stg__generated_reviews`
- `stg__amenities_changelog`

#### 3. Intermediate

Business logic and enrichment happen here:

- `int_daily_amenities`: maps point-in-time amenity changelog to daily amenity sets.
- `int_daily_reviews`: aggregates review events to the daily grain.
- `int_calendar_enriched`: joins calendar backbone with listings, amenities, and reviews at daily grain.

#### 4. Marts

Final curated datasets designed for analysis and reporting. The primary mart in this project:

- `rental__listing_daily_metrics`: a daily fact table containing revenue, availability, pricing, amenities, and review metrics.

#### 5. Analyses Layer

This layer contains non-materialized SQL used for reference, ad hoc analytical queries, and solving the business questions provided in the assessment.
These models do not build tables in the warehouse; instead, they serve as documented analytical logic for stakeholders or reviewers.

Analysis models include:

  - `amenity_revenue` — Monthly revenue segmentation by AC vs no AC

  - `maximum_stay_possible` — Longest continuous availability window per listing

  - `maximum_stay_lockbox_firstaid` — Longest stay windows constrained to listings that have both Lockbox AND First Aid Kit

  - `neighbourhood_pricing_change` — Neighborhood-level price change analysis across a 1-year period

The analyses folder demonstrates how the curated mart enables simple, expressive SQL for real business questions.
