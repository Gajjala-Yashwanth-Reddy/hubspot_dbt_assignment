WITH calendar AS (

  SELECT *
  FROM {{ ref('stg__calendar') }}

),

listings AS (

  SELECT *
  FROM {{ ref('stg__listings') }}

),

amenities AS (

  SELECT *
  FROM {{ ref('int_daily_amenities') }}

),

reviews AS (

  SELECT *
  FROM {{ ref('int_daily_reviews') }}

),

joined AS (

  SELECT
    c.listing_id,
    c.date_day,
    c.is_available,
    c.reservation_id,
    c.day_price,
    c.minimum_nights,
    c.maximum_nights,

    l.name AS listing_name,
    l.neighborhood,
    l.property_type,
    l.room_type,
    l.accommodates,
    l.bathrooms_text,
    l.bedrooms,
    l.beds,
    l.base_price,
    l.review_rating AS overall_review_rating,
    a.amenities,
    r.avg_review_score
  FROM calendar AS c
  LEFT JOIN listings AS l
    ON c.listing_id = l.listing_id
  LEFT JOIN amenities AS a
    ON
      c.listing_id = a.listing_id
      AND c.date_day = a.date_day
  LEFT JOIN reviews AS r
    ON
      c.listing_id = r.listing_id
      AND c.date_day = r.review_date

)

SELECT * FROM joined
