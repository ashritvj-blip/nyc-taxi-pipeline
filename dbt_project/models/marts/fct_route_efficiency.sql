WITH trips AS (
    SELECT * FROM {{ ref('int_trips_enriched') }}
)

SELECT
    route_key,
    pickup_location_id,
    dropoff_location_id,
    COUNT(*)                                        AS total_trips,
    ROUND(AVG(trip_distance), 2)                    AS avg_distance_miles,
    ROUND(AVG(trip_duration_mins), 2)               AS avg_duration_mins,
    ROUND(AVG(trip_duration_mins / NULLIF(trip_distance, 0)), 2)
                                                    AS mins_per_mile,
    ROUND(AVG(total_amount), 2)                     AS avg_fare
FROM trips
WHERE trip_distance > 0
  AND trip_duration_mins > 0
  AND trip_duration_mins < 180
GROUP BY
    route_key,
    pickup_location_id,
    dropoff_location_id
HAVING COUNT(*) >= 10
ORDER BY
    mins_per_mile DESC