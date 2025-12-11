with prices as (
    select
        neighborhood,
        listing_id,
        max(case when date_day = '2021-07-12' then day_price end) as price_2021,
        max(case when date_day = '2022-07-11' then day_price end) as price_2022
    from {{ ref('rental__listing_daily_metrics') }}
    group by 1, 2
)

select
    neighborhood,
    avg(price_2022 - price_2021) as avg_price_increase
from prices
group by 1
order by neighborhood
