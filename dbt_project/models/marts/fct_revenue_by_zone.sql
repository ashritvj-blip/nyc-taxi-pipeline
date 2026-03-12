WITH trips AS (
    SELECT * FROM {{ ref('int_trips_enriched') }}
)

SELECT
    pickup_location_id                          AS zone_id,
    pickup_hour,
    pickup_day,
    COUNT(*)                                    AS total_trips,
    ROUND(SUM(total_amount), 2)                 AS total_revenue,
    ROUND(AVG(total_amount), 2)                 AS avg_revenue_per_trip,
    ROUND(SUM(total_amount) / COUNT(*), 2)      AS revenue_per_trip
FROM trips
GROUP BY
    pickup_location_id,
    pickup_hour,
    pickup_day
ORDER BY
    total_revenue DESC