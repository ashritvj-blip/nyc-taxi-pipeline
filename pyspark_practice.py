from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, round

# Step 1 — Create a Spark Session
spark = SparkSession.builder \
    .appName("NYC Taxi PySpark Practice") \
    .getOrCreate()

print("✅ Spark Session created!")

# Step 2 — Read the bronze Parquet files
df = spark.read.parquet("/Users/sumedhatvns/nyc-taxi-pipeline/data/bronze/*.parquet")

print("✅ Data loaded!")
print(f"Total rows: {df.count()}")

# Step 3 — See the schema
df.printSchema()

# Step 4 — Show first 5 rows
df.show(5)

from pyspark.sql.functions import col, sum, avg, count, round, when

# Step 5 — Filter: only trips with fare > 10
df_filtered = df.filter(col("fare_amount") > 10)
print(f"Trips with fare > $10: {df_filtered.count()}")

# Step 6 — Add a new column: fare per mile
df_enriched = df_filtered.withColumn(
    "fare_per_mile",
    round(col("fare_amount") / col("trip_distance"), 2)
)

# Step 7 — Group by payment type and aggregate
df_grouped = df_enriched.groupBy("payment_type") \
    .agg(
        count("*").alias("total_trips"),
        round(sum("fare_amount"), 2).alias("total_fare"),
        round(avg("tip_amount"), 2).alias("avg_tip")
    )

df_grouped.show()

# Step 8 — Add payment description using when (like CASE WHEN in SQL)
df_final = df_grouped.withColumn("payment_desc",
    when(col("payment_type") == 1, "Credit Card")
    .when(col("payment_type") == 2, "Cash")
    .when(col("payment_type") == 3, "No Charge")
    .otherwise("Other")
)

df_final.show()

from pyspark.sql.functions import row_number, rank, sum as spark_sum
from pyspark.sql.window import Window

# Define a window — partition by payment type, order by fare amount
window = Window.partitionBy("payment_type").orderBy(col("fare_amount").desc())

# Add ROW_NUMBER and RANK
df_window = df.filter(col("fare_amount") > 10) \
              .withColumn("row_num", row_number().over(window)) \
              .withColumn("rank", rank().over(window))

# Show top 3 trips per payment type
df_window.filter(col("row_num") <= 3) \
         .select("payment_type", "fare_amount", "trip_distance", "row_num", "rank") \
         .orderBy("payment_type", "row_num") \
         .show()

# Running total of fare per payment type
window2 = Window.partitionBy("payment_type") \
                .orderBy("tpep_pickup_datetime") \
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_running = df.filter(col("fare_amount") > 10) \
               .withColumn("running_total", round(spark_sum("fare_amount").over(window2), 2))

df_running.select("payment_type", "tpep_pickup_datetime", "fare_amount", "running_total") \
          .filter(col("payment_type") == 1) \
          .show(5)

# ── JOINS ──────────────────────────────────────────────────────

# Read the zones lookup file
df_zones = spark.read.csv(
    "/Users/sumedhatvns/nyc-taxi-pipeline/data/taxi_zones.csv",
    header=True,
    inferSchema=True
)

print("Zones data:")
df_zones.show(5)

# Inner Join — trips with their pickup zone names
df_joined = df.join(
    df_zones,
    df.PULocationID == df_zones.LocationID,
    "inner"
)

# Select only useful columns
df_joined = df_joined.select(
    "tpep_pickup_datetime",
    "fare_amount",
    "tip_amount",
    "trip_distance",
    "payment_type",
    "Zone",
    "Borough"
)

print("Trips with zone names:")
df_joined.show(5)

# Now aggregate — top 5 boroughs by total revenue
df_borough = df_joined.groupBy("Borough") \
    .agg(
        count("*").alias("total_trips"),
        round(sum("fare_amount"), 2).alias("total_revenue"),
        round(avg("tip_amount"), 2).alias("avg_tip")
    ) \
    .orderBy(col("total_revenue").desc())

print("Revenue by Borough:")
df_borough.show()

# ── SPARK SQL ──────────────────────────────────────────────────

# Register DataFrames as temporary SQL tables
df.createOrReplaceTempView("trips")
df_zones.createOrReplaceTempView("zones")

# Write pure SQL!
result = spark.sql("""
    SELECT 
        z.Borough,
        COUNT(*) AS total_trips,
        ROUND(SUM(t.fare_amount), 2) AS total_revenue,
        ROUND(AVG(t.tip_amount), 2) AS avg_tip
    FROM trips t
    INNER JOIN zones z ON t.PULocationID = z.LocationID
    GROUP BY z.Borough
    ORDER BY total_revenue DESC
""")

print("Spark SQL Result:")
result.show()