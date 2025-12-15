WITH changes AS (

  SELECT
    listing_id,
    change_ts,
    amenities,
    lead(change_ts) OVER (
      PARTITION BY listing_id
      ORDER BY change_ts
    ) AS next_change_ts
  FROM {{ ref('stg__amenities_changelog') }}

),

calendar AS (

  SELECT
    listing_id,
    date_day
  FROM {{ ref('stg__calendar') }}

),

mapped AS (

  SELECT
    c.listing_id,
    c.date_day,
    ch.amenities
  FROM calendar AS c
  LEFT JOIN changes AS ch
    ON
      c.listing_id = ch.listing_id
      AND c.date_day >= cast(ch.change_ts AS date)
      AND (
        ch.next_change_ts IS null
        OR c.date_day < cast(ch.next_change_ts AS date)
      )

)

SELECT * FROM mapped
