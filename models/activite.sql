{{ config(materialized='table') }}


WITH source_listings AS (
    SELECT
        *
    FROM {{ source("staging","listings_airbnb_paris") }}
),

filtre_last_month AS (
    SELECT *
FROM (
  SELECT
    *,
    CASE
      WHEN last_review IS NULL OR last_review = 'null' THEN CURRENT_DATE()
      ELSE PARSE_DATE('%Y-%m-%d', last_review)
    END AS last_review_date
FROM source_listings
)
WHERE 
  last_review_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)

),
cal_availability_365 AS (
SELECT
    availability_365,
    CASE
        WHEN availability_365 = 0 THEN '0'
        WHEN availability_365 IS NULL THEN '0'
        WHEN availability_365 BETWEEN 1 AND 30 THEN '1-30'
        WHEN availability_365 BETWEEN 31 AND 60 THEN '31-60'
        WHEN availability_365 BETWEEN 61 AND 90 THEN '61-90'
        WHEN availability_365 BETWEEN 91 AND 120 THEN '91-120'
        WHEN availability_365 BETWEEN 121 AND 150 THEN '121-150'
        WHEN availability_365 BETWEEN 151 AND 180 THEN '151-180'
        WHEN availability_365 BETWEEN 181 AND 210 THEN '181-210'
        WHEN availability_365 BETWEEN 211 AND 240 THEN '211-240'
        WHEN availability_365 >= 241 THEN '241-255+'
    END AS groupe
FROM source_listings

),
group_availability_365 AS (
     SELECT
        groupe,
        COUNT(*) AS count_groupe
    FROM cal_availability_365
    GROUP BY groupe
)
SELECT 
 *
 FROM group_availability_365