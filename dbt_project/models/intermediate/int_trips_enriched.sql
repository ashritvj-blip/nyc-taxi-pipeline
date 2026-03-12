WITH trips AS (
    SELECT * FROM {{ ref('stg_taxi_trips') }}
),

enriched AS (
    SELECT
        *,
        -- Payment type description
        CASE payment_type
            WHEN 1 THEN 'Credit Card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No Charge'
            WHEN 4 THEN 'Dispute'
            ELSE 'Unknown'
        END AS payment_type_desc,

        -- Time of day buckets
        CASE
            WHEN pickup_hour BETWEEN 6  AND 11 THEN 'Morning'
            WHEN pickup_hour BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN pickup_hour BETWEEN 17 AND 20 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day,

        -- Tip percentage
        CASE
            WHEN fare_amount > 0
            THEN ROUND((tip_amount / fare_amount) * 100, 2)
            ELSE 0
        END AS tip_pct,

        -- Route key
        CONCAT(pickup_location_id, '_', dropoff_location_id) AS route_key

    FROM trips
)

SELECT * FROM enriched