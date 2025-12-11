WITH source AS (

  SELECT * FROM {{ source('raw', 'generated_reviews') }}

),

cleaned AS (

  SELECT
    id AS review_id,
    listing_id,
    cast(review_score AS numeric) AS review_score,
    cast(review_date AS date) AS review_date
  FROM source

)

SELECT * FROM cleaned
