{{ config(materialized='table') }}

WITH source_listings AS (
    SELECT
        room_type
    FROM {{ source("staging","listings_airbnb_paris") }}
),
counts_type AS (
    SELECT
        room_type,
        COUNT(room_type) AS count_room_type
    FROM source_listings
    GROUP BY room_type
),
cal_proportion AS (
SELECT
    *,
    ROUND((count_room_type / SUM(count_room_type) OVER ()) * 100, 1) AS proportion
FROM counts_type
)

SELECT 
 *
 FROM cal_proportion
ORDER BY count_room_type DESC
