{{ config(
    materialized="table"
)}}


WITH source_listings AS(
SELECT 
    minimum_nights
    FROM {{ source("staging","listings_airbnb_paris") }}
),
count_groupe AS (
    SELECT 
        minimum_nights, 
       COUNT(*) as linstings_nuit
    FROM  source_listings
    GROUP BY minimum_nights 
),
mois_35 AS(
    SELECT
    minimum_nights,
    linstings_nuit,
    CASE
    WHEN minimum_nights >= 35 THEN '35'
    ELSE CAST(minimum_nights AS STRING)
   END AS minimum
  FROM count_groupe
),
count_groupe_moins AS (
    SELECT 
        minimum, 
       SUM(linstings_nuit) as linstings_nuit_group
    FROM  mois_35
    GROUP BY minimum
),cal_total AS(
    SELECT
    minimum,
    linstings_nuit_group,
    SUM(CASE WHEN CAST(minimum AS INT) < 30 THEN linstings_nuit_group ELSE 0 END) OVER () AS total_avant_30,
    SUM(CASE WHEN CAST(minimum AS INT) >= 30 THEN linstings_nuit_group ELSE 0 END) OVER () AS total_apres_30,
FROM count_groupe_moins
),
cal_proportion AS (
SELECT  
    minimum,
    CASE 
        WHEN CAST(minimum AS INT) >= 35 THEN '35+'
        ELSE CAST(minimum AS STRING)
    END AS minimum_nights,
    linstings_nuit_group,
    total_avant_30,
    total_apres_30,
    ROUND((total_avant_30 / SUM(linstings_nuit_group) OVER ()) * 100, 1) AS proportion_avnt_30
FROM cal_total
)


SELECT 
    minimum_nights,
    linstings_nuit_group,
    total_avant_30,
    total_apres_30,
    proportion_avnt_30
FROM cal_proportion
ORDER BY minimum_nights
