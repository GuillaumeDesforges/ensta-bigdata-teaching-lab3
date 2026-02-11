# Exercise 8: Count how many unique aircraft (by hex identifier) were tracked during the day

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

spark = SparkSession.builder.getOrCreate()

# Read parquet data
df = spark.read.parquet("tmp/data/parquet")

# =============================================================================
# Solution using DataFrame API
# =============================================================================

print("=== DataFrame API ===")

unique_count_df = df.select(countDistinct(col("aircraft.hex")).alias("unique_aircraft"))
unique_count_df.show()

# Alternative using groupBy
unique_count_df_alt = df.select("aircraft.hex").distinct().count()
print(f"Unique aircraft count (alternative): {unique_count_df_alt}")

# =============================================================================
# Solution using RDD API
# =============================================================================

print("\n=== RDD API ===")

unique_count_rdd = (
    df.rdd.map(lambda row: row.aircraft.hex)
    .filter(lambda x: x is not None)
    .distinct()
    .count()
)

print(f"Unique aircraft count: {unique_count_rdd}")
