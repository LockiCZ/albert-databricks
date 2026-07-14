import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ingest")

parser = argparse.ArgumentParser()
parser.add_argument("--landing_volume")
parser.add_argument("--bronze_container")
parser.add_argument("--checkpoint_container")
parser.add_argument("--layer_name")
parser.add_argument("--table_schema")
parser.add_argument("--table_name")
args = parser.parse_args()

landing_volume       = args.landing_volume
bronze_container     = args.bronze_container
checkpoint_container = args.checkpoint_container
layer_name           = args.layer_name
table_schema         = args.table_schema
table_name           = args.table_name

checkpoint = f"{checkpoint_container}/{layer_name}/{table_schema}/{table_name}"
table = f"{layer_name}.{table_schema}.{table_name}"
source_path = f"{landing_volume}/{table_name}"

logger.info(
    "Starting ingestion: source=%s, target=%s",
    source_path,
    table,
)

# Create external Delta table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table}
    USING DELTA
    LOCATION '{bronze_container}/external/{table_schema}/{table_name}'
""")
logger.info("Target table verified/created: %s", table)

logger.info("Reading CSV files from %s with autoloader", source_path)
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", checkpoint)
    .option("header", "true")
    .load(source_path)
)

df = (
    df
    .withColumn("_INGESTION_TIME", F.current_timestamp())
    .withColumn("_SOURCE_FILE", F.col("_metadata.file_path"))
)

logger.info("Starting streaming write to %s (availableNow mode)", table)
query = (
    df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(table)
)

query.awaitTermination()
logger.info("Ingestion completed: %s", table)