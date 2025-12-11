WITH base AS (

  SELECT
    listing_id,
    review_date,
    avg(review_score) AS avg_review_score,
    min(review_score) AS min_review_score,
    max(review_score) AS max_review_score
  FROM {{ ref('stg__generated_reviews') }}
  GROUP BY listing_id, review_date

)

SELECT * FROM base
