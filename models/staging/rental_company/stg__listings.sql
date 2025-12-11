WITH source AS (

  SELECT * FROM {{ source('raw', 'listings') }}

),

renamed AS (

  SELECT
    id AS listing_id,
    name,
    host_id,
    host_name,
    cast(host_since AS date) AS host_since,
    host_location,
    host_verifications,
    neighborhood,
    property_type,
    room_type,
    accommodates,
    bathrooms_text,
    bedrooms,
    beds,
    cast(replace(price, '$', '') AS numeric) AS base_price,
    amenities,
    number_of_reviews,
    cast(first_review AS date) AS first_review_date,
    cast(last_review AS date) AS last_review_date,
    cast(review_scores_rating AS numeric) AS review_rating
  FROM source

)

SELECT * FROM renamed
