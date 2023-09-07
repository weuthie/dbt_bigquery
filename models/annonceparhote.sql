{{ config(
    materialized="table"
)}}

WITH source_listings AS (
    SELECT
        host_id,
        room_type
    FROM {{ source("staging","listings_airbnb_paris") }}
),
source_host AS(
    SELECT
        *
    FROM {{ source("staging","hosts_airbnb_paris") }}
),
source_join AS (
    SELECT
    h.host_id,
    l.room_type,
    h.host_name
FROM source_listings AS l
JOIN source_host AS h ON l.host_id = h.host_id

),
top_host as (
select
    host_id,
    host_name,
    sum(case when room_type = 'Entire home/apt' then 1 else 0 end) as home_apts,
    SUM(case when room_type = 'Private room' then 1 else 0 end) as private_rooms,
    SUM(case when room_type = 'Shared room' then 1 else 0 end) as shared_rooms,
    SUM(case when room_type = 'Hotel Room' then 1 else 0 end) as hotel_rooms,
    SUM(case when room_type IN ('Entire home/apt', 'Private room', 'Shared room', 'Hotel Room') then 1 else 0 end) as total_rooms
from
    source_join
group by
    host_id ,host_name
order by total_rooms desc
)

select * from top_host
order by total_rooms desc
