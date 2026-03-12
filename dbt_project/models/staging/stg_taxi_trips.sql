WITH source AS (
    SELECT *
    FROM read_parquet('../data/bronze/yellow_tripdata_*.parquet')
),

cleaned AS (
    SELECT
        VendorID                                    AS vendor_id,
        tpep_pickup_datetime                        AS pickup_datetime,
        tpep_dropoff_datetime                       AS dropoff_datetime,
        passenger_count                             AS passenger_count,
        trip_distance,
        PULocationID                                AS pickup_location_id,
        DOLocationID                                AS dropoff_location_id,
        payment_type,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        congestion_surcharge,

        -- Calculated columns
        ROUND(
            EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60
        , 2)                                        AS trip_duration_mins,

        EXTRACT(HOUR FROM tpep_pickup_datetime)     AS pickup_hour,
        DAYNAME(tpep_pickup_datetime)               AS pickup_day,
        CAST(tpep_pickup_datetime AS DATE)          AS pickup_date

    FROM source

    -- Basic data quality filters
    WHERE fare_amount       > 0
      AND trip_distance     > 0
      AND passenger_count   > 0
      AND tpep_pickup_datetime  >= '2024-01-01'
      AND tpep_pickup_datetime  <  '2024-04-01'
)

SELECT * FROM cleaned
