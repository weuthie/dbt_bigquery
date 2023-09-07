{{ config(materialized='table') }}

WITH source_listings AS (
    SELECT
        license
    FROM {{ source("staging","listings_airbnb_paris") }}
),
type_licence AS (
  SELECT
    CASE
      WHEN license LIKE '%Available%' THEN 'Exempt' 
      WHEN license  LIKE '%Exempt%' THEN 'Exempt'
      WHEN license LIKE '%null%' THEN 'unlicensed'
     WHEN (license NOT LIKE '%Available%' AND license NOT LIKE '%Exempt%')  AND NOT REGEXP_CONTAINS(license, r'\d+')
    THEN 'license'
      WHEN REGEXP_CONTAINS(license, r'\d+') THEN 'license'
      ELSE 'pending'
    END AS license_status
  FROM source_listings
),
count_license AS(
    SELECT
        license_status,
        count(*) AS count_listings
    FROM type_licence
    GROUP BY license_status

),
cal_proportion AS (
SELECT
    *,
    ROUND((count_listings / SUM(count_listings) OVER ()) * 100, 1) AS proportion
FROM count_license
)

SELECT 
 *
 FROM cal_proportion
ORDER BY count_listings DESC
