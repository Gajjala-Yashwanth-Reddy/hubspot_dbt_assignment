WITH source AS (

  SELECT * FROM {{ source('raw', 'calendar') }}

),

cleaned AS (

  SELECT
    listing_id,
    cast(date AS date) AS date_day,
    reservation_id,
    cast(replace(price, '$', '') AS numeric) AS day_price,
    minimum_nights,
    maximum_nights,
    (available = 't') AS is_available
  FROM source

)

SELECT * FROM cleaned
