{{ config(
    materialized="table"
)}}


WITH source_listings AS(
SELECT 
    host_id
    FROM {{ source("staging","listings_airbnb_paris") }}
),
count_groupe AS (
    SELECT 
        host_id, 
        COUNT(*) as count_host
    FROM  source_listings
    GROUP BY host_id 
),
mois_10 AS(
    SELECT
    host_id,
    count_host,
    CASE
    WHEN count_host >= 10 THEN '10'
    ELSE CAST(count_host AS STRING)
  END AS total_host
  FROM count_groupe
),
count_groupe_moins AS (
    SELECT 
        total_host, 
        SUM(count_host) as sum_total
    FROM  mois_10
    GROUP BY total_host
),cal_total AS(
    SELECT
    total_host,
    sum_total,
    SUM(CASE WHEN CAST(total_host AS INT) =1 THEN sum_total ELSE 0 END) OVER () AS single_listings,
    SUM(CASE WHEN CAST(total_host AS INT) >1 THEN sum_total ELSE 0 END) OVER () AS multi_listings,
FROM count_groupe_moins
),
cal_proportion AS (
SELECT  
    total_host,
    CASE 
        WHEN CAST(total_host AS INT) >= 35 THEN '10+'
        ELSE CAST(total_host AS STRING)
    END AS total_listings,
    sum_total,
    single_listings,
    multi_listings,
    ROUND((single_listings / SUM(sum_total) OVER ()) * 100, 1) AS proportion_single_listings
FROM cal_total
)


SELECT 
    *
FROM cal_proportion
ORDER BY total_host

