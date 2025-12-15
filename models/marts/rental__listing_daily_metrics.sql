WITH base AS (

  SELECT
    listing_id,
    date_day,
    is_available,
    reservation_id,
    day_price,
    minimum_nights,
    maximum_nights,
    listing_name,
    neighborhood,
    property_type,
    room_type,
    accommodates,
    bathrooms_text,
    bedrooms,
    beds,
    base_price,
    overall_review_rating,
    amenities,
    avg_review_score
  FROM {{ ref('int_daily_calendar') }}

),

final AS (

  SELECT
    listing_id,
    date_day,
    listing_name,
    neighborhood,
    property_type,
    room_type,
    accommodates,
    bathrooms_text,
    bedrooms,
    beds,
    is_available,
    reservation_id,
    day_price,
    minimum_nights,
    maximum_nights,
    avg_review_score,
    overall_review_rating,
    amenities,
    CASE
      WHEN reservation_id IS NOT NULL THEN day_price
      ELSE 0
    END AS revenue,
    (reservation_id IS NOT NULL) AS is_occupied,
    coalesce(amenities LIKE '%"Air conditioning"%', false) AS has_ac,
    coalesce(amenities LIKE '%"Lockbox"%', false) AS has_lockbox,
    coalesce(amenities LIKE '%"First aid kit"%', false) AS has_first_aid_kit
  FROM base

)

SELECT * FROM final
