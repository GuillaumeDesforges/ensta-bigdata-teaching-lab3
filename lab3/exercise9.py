# Exercise 9: Create a visualization using Altair to show the distribution
# of aircraft by category

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import altair as alt
import pandas as pd

spark = SparkSession.builder.getOrCreate()

# Read parquet data
df = spark.read.parquet("tmp/data/parquet")

# =============================================================================
# Solution using DataFrame API
# =============================================================================

print("=== DataFrame API ===")

category_counts_df = (
    df.select("aircraft.hex", "aircraft.category")
    .filter(col("category").isNotNull())
    .distinct()  # Count each aircraft once
    .groupBy("category")
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)

category_counts_df.show()

# Convert to Pandas for Altair
pandas_df = category_counts_df.toPandas()

# =============================================================================
# Solution using RDD API
# =============================================================================

print("\n=== RDD API ===")

category_counts_rdd = (
    df.rdd.map(lambda row: (row.aircraft.hex, row.aircraft.category))
    .filter(lambda x: x[1] is not None)
    .distinct()  # Count each aircraft once
    .map(lambda x: (x[1], 1))  # (category, 1)
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: -x[1])
    .collect()
)

print("category | count")
print("-" * 20)
for category, cnt in category_counts_rdd:
    print(f"{category:<8} | {cnt}")

# Convert RDD result to Pandas for comparison
pandas_rdd_df = pd.DataFrame(category_counts_rdd, columns=["category", "count"])

# =============================================================================
# Altair Visualization (using DataFrame API result)
# =============================================================================

chart = (
    alt.Chart(pandas_df)
    .mark_bar()
    .encode(
        x=alt.X("category:N", sort="-y", title="Aircraft Category"),
        y=alt.Y("count:Q", title="Number of Aircraft"),
        tooltip=["category", "count"],
    )
    .properties(title="Distribution of Aircraft by Category", width=600, height=400)
)

# Save chart to HTML file
chart.save("tmp/data/category_distribution.html")
print("\nChart saved to tmp/data/category_distribution.html")
