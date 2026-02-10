# Lab 3: Apache Spark

## Setup

Create a virtual environment and install dependencies.

```bash
python3 -m venv .venv --copies
source .venv/bin/activate
pip install -e .
```

Download data from ADS-B Exchange (aircraft tracking data).

```bash
mkdir -p tmp/data/raw/download
python3 download.py --day 2025-01-01 --output-dir tmp/data/raw/download/
```

Decompress the downloaded files.

```bash
find tmp/data/raw/download/ -name '*.json.gz' | xargs -P 100 gzip -vkd
```

## Data Ingestion with PySpark

Open `__main__.py` and implement the functions marked with `START STUDENT CODE` / `END STUDENT CODE`.

### Exercise 1: Create a Spark Session

Implement `get_spark()` to return a SparkSession.

### Exercise 2: First Read with Schema Inference

Implement `first_read()` to:
- Read all JSON files from the data directory
- Print the inferred schema
- Save the schema to a JSON file

### Exercise 3: Read with Predefined Schema

Implement `next_read()` to:
- Load the schema from the JSON file
- Read the JSON files using the predefined schema

> Question: Why is it beneficial to save and reuse the schema instead of inferring it every time?

### Exercise 4: Explode Nested Data

Implement `explode_rows()` to transform the DataFrame by exploding the `aircraft` array column into individual rows.

### Exercise 5: Write to Parquet

Implement `write_to_parquet()` to write the DataFrame to Parquet format with overwrite mode.

### Exercise 6: Read from Parquet

Implement `read_parquet()` to read data back from the Parquet files.

> Question: Compare the size of the Parquet files with the original JSON files. What compression ratio do you observe?

## Data Analysis

> Exercise 7: Using the Parquet data, write a Spark query to find the 10 aircraft with the highest altitude (`alt_baro`). What types of aircraft are they?

> Exercise 8: Write a query to count how many unique aircraft (by `hex` identifier) were tracked during the day.

> Exercise 9: Create a visualization using Altair to show the distribution of aircraft by category (`category` field).
