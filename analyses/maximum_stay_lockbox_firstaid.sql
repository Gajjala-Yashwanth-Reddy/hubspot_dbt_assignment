with filtered as (
    select
        listing_id,
        date_day
    from {{ ref('rental__listing_daily_metrics') }}
    where is_available = true
      and has_lockbox = true
      and has_first_aid_kit = true
),

-- 2. Assign each row a row_number so we can detect gaps-in-dates for each listing
numbered as (
    select
        listing_id,
        date_day,
        row_number() over (partition by listing_id order by date_day) as rn
    from filtered
),

-- 3. Create "islands" by subtracting rn from the date — this creates equal "grp" values for consecutive dates
islands as (
    select
        listing_id,
        date_day,
        date_day - rn * interval '1' day as grp
    from numbered
),

-- 4. Aggregate each island into a contiguous block with start, end, and number of nights
contiguous AS (
    select
        listing_id,
        min(date_day) as start_date,
        max(date_day) as end_date,
        count(*) as nights
    from islands
    group by listing_id, grp
)

-- 5. Final result
select *
from contiguous
order by nights desc, listing_id
