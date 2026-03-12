WITH trips AS (
    SELECT * FROM {{ ref('int_trips_enriched') }}
)

SELECT
    payment_type_desc,
    time_of_day,
    COUNT(*)                                AS total_trips,
    ROUND(AVG(tip_pct), 2)                  AS avg_tip_pct,
    ROUND(AVG(tip_amount), 2)               AS avg_tip_amount,
    ROUND(MIN(tip_pct), 2)                  AS min_tip_pct,
    ROUND(MAX(tip_pct), 2)                  AS max_tip_pct
FROM trips
WHERE payment_type_desc IN ('Credit Card', 'Cash')
GROUP BY
    payment_type_desc,
    time_of_day
ORDER BY
    avg_tip_pct DESC