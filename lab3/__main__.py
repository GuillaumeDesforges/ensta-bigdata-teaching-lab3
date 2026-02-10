# %%
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import json
from pathlib import Path


# %%
def get_spark() -> SparkSession:
    """
    Instructions:
    - Return a Spark session
    """
    # START STUDENT CODE
    # END STUDENT CODE


spark = get_spark()


# %%
def first_read(data_file_paths: list[str], schema_path: str) -> DataFrame:
    """
    Instructions:
    - Read all json files in tmp/data/raw/download/
    - Print schema
    - Save schema to tmp/data/schema.json
    """
    # START STUDENT CODE
    # END STUDENT CODE


DATA_PATH = Path("tmp/data/raw/download")
assert DATA_PATH.exists(), f"{DATA_PATH} does not exist"
DATA_FILE_PATHS = [str(p) for p in DATA_PATH.glob("*.json")]
assert DATA_FILE_PATHS, "No json files found in data path"

SCHEMA_PATH = Path("tmp/data/schema.json")

DF_RAW_FIRST = first_read(data_file_paths=DATA_FILE_PATHS, schema_path=SCHEMA_PATH)

assert DF_RAW_FIRST is not None, (
    "Returned result is None, does the function return anything?"
)
assert isinstance(DF_RAW_FIRST, DataFrame), "DataFrame is not a valid DataFrame"
assert SCHEMA_PATH.exists(), f"{SCHEMA_PATH} does not exist"


# %%
def next_read(data_file_paths: list[str], schema_path: str) -> DataFrame:
    """
    Instructions:
    - Read schema from file
    - Read all json files in tmp/data/raw/download/
    - Return dataframe
    """
    # START STUDENT CODE
    # END STUDENT CODE


DF_RAW = next_read(data_file_paths=DATA_FILE_PATHS, schema_path=SCHEMA_PATH)
DF_RAW.printSchema()
print("DF_RAW.count()", DF_RAW.count())  # should be 17280


# %%
def explode_rows(df: DataFrame) -> DataFrame:
    """
    Instructions:
    - Explode aircraft column into multiple rows
    """
    # START STUDENT CODE
    # END STUDENT CODE


DF_RAW_EXPLODED = explode_rows(df=DF_RAW)

# %%
DF_RAW_EXPLODED.printSchema()
print("DF_RAW_EXPLODED.count()", DF_RAW_EXPLODED.count())


# %%
def write_to_parquet(df: DataFrame, output_path: str):
    """
    Instructions:
    - Write (mode "overwrite") dataframe to parquet file
    """
    # START STUDENT CODE
    # END STUDENT CODE


OUTPUT_PATH = Path("tmp/data/parquet")
assert OUTPUT_PATH.parent.exists(), f"{OUTPUT_PATH.parent} does not exist"

write_to_parquet(df=DF_RAW_EXPLODED, output_path=str(OUTPUT_PATH))
assert OUTPUT_PATH.exists(), f"Output path {OUTPUT_PATH} does not exist"


# %%
def read_parquet(input_path: str) -> DataFrame:
    """
    Instructions:
    - Read parquet file
    """
    # START STUDENT CODE
    # END STUDENT CODE


DF_PARQUET = read_parquet(input_path=str(OUTPUT_PATH))


# %%
DF_PARQUET.printSchema()
print("DF_PARQUET.count()", DF_PARQUET.count())
