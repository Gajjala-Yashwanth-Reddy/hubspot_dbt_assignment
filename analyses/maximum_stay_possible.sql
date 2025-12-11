with available as (
    select
        listing_id,
        date_day,
        row_number() over (partition by listing_id order by date_day) as rn
    from rental_analytics.marts.rental__listing_daily_metrics
    where is_available = true
)
, islands as (
    select
        listing_id,
        date_day,
        date_day - rn * interval '1' day as grp
    from available
), contiguous as (
    select
        listing_id,
        min(date_day) as start_date,
        max(date_day) as end_date,
        count(*) as nights   
    from islands
    group by listing_id, grp
)

select *
from contiguous
order by nights desc, listing_id
