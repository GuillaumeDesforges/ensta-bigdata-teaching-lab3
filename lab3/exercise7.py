# Exercise 7: Find the 10 aircraft with the highest altitude (alt_baro)
# What types of aircraft are they?

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# Read parquet data
df = spark.read.parquet("tmp/data/parquet")

# =============================================================================
# Solution using DataFrame API
# =============================================================================

print("=== DataFrame API ===")

df_result = (
    df.select("aircraft.hex", "aircraft.flight", "aircraft.t", "aircraft.alt_baro")
    .filter(col("alt_baro").isNotNull())
    .filter(col("alt_baro") != "ground")
    .orderBy(col("alt_baro").cast("int").desc())
    .limit(10)
)

df_result.show(truncate=False)

# =============================================================================
# Solution using RDD API
# =============================================================================

print("=== RDD API ===")


def parse_aircraft(row):
    aircraft = row.aircraft
    alt_baro = aircraft.alt_baro
    # Filter out None and "ground" values
    if alt_baro is None or alt_baro == "ground":
        return None
    try:
        alt = int(alt_baro)
    except (ValueError, TypeError):
        return None
    return (alt, aircraft.hex, aircraft.flight, aircraft.t)


rdd_result = (
    df.rdd.map(parse_aircraft)
    .filter(lambda x: x is not None)
    .sortBy(lambda x: -x[0])  # Sort by altitude descending
    .take(10)
)

print("alt_baro | hex    | flight   | type")
print("-" * 40)
for alt, hex_id, flight, aircraft_type in rdd_result:
    print(f"{alt:>7} | {hex_id or '':<6} | {flight or '':<8} | {aircraft_type or ''}")
