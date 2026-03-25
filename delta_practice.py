from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session with Delta Lake support
spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("NYC Taxi Delta Lake")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

print("✅ Spark Session with Delta Lake ready!")

# Read the bronze parquet files
df = spark.read.parquet("/Users/sumedhatvns/nyc-taxi-pipeline/data/bronze/*.parquet")

# Write as a Delta table
df.write \
  .format("delta") \
  .mode("overwrite") \
  .save("/Users/sumedhatvns/nyc-taxi-pipeline/data/delta/taxi_trips")

print("✅ Delta table written!")

# Read it back
df_delta = spark.read.format("delta") \
    .load("/Users/sumedhatvns/nyc-taxi-pipeline/data/delta/taxi_trips")

print(f"Total rows in Delta table: {df_delta.count()}")
df_delta.show(5)

# ── DELTA LAKE KILLER FEATURES ─────────────────────────────────

# FEATURE 1 — Time Travel (see previous versions)
df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("/Users/sumedhatvns/nyc-taxi-pipeline/data/delta/taxi_trips")

print(f"Version 0 row count: {df_v0.count()}")

# FEATURE 2 — View transaction history
spark.sql("""
    DESCRIBE HISTORY delta.`/Users/sumedhatvns/nyc-taxi-pipeline/data/delta/taxi_trips`
""").select("version", "timestamp", "operation").show()

# FEATURE 3 — Update rows (impossible with regular Parquet!)
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(
    spark,
    "/Users/sumedhatvns/nyc-taxi-pipeline/data/delta/taxi_trips"
)

# Update all rows where payment_type is 0 to 5
delta_table.update(
    condition = col("payment_type") == 0,
    set = {"payment_type": "5"}
)

print("✅ Rows updated!")