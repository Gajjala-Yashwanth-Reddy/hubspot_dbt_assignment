WITH source AS (

  SELECT * FROM {{ source('raw', 'amenities_changelog') }}

),

cleaned AS (

  SELECT
    listing_id,
    cast(change_at AS timestamp) AS change_ts,
    amenities
  FROM source

)

SELECT * FROM cleaned
