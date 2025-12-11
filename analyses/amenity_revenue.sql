with revenue_by_ac as (
    select
        date_trunc('month', date_day) as month,
        has_ac,
        sum(revenue) as total_revenue
    from {{ ref('rental__listing_daily_metrics') }}
    group by 1, 2
)

select
    month,
    has_ac,
    total_revenue,
    round(total_revenue * 100.0 / sum(total_revenue) over (partition by month), 2) as pct_revenue
from revenue_by_ac
order by month, has_ac
