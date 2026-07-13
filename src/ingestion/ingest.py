import argparse
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--landing_volume")
parser.add_argument("--bronze_container")
parser.add_argument("--checkpoint_container")
parser.add_argument("--layer_name")
parser.add_argument("--table_schema")
parser.add_argument("--table_name")
args = parser.parse_args()

landing_volume    = args.landing_volume
bronze_container     = args.bronze_container
checkpoint_container = args.checkpoint_container
layer_name      = args.layer_name
table_schema    = args.table_schema
table_name      = args.table_name

checkpoint = f"{checkpoint_container}/{layer_name}/{table_schema}/{table_name}"

# Create external Delta table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {layer_name}.{table_schema}.{table_name}
    USING DELTA
    LOCATION '{bronze_container}/external/{table_schema}/{table_name}'
""")

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", checkpoint)
    .option("header", "true")
    .load(f"{landing_volume}/{table_name}")
)

(
    df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{layer_name}.{table_schema}.{table_name}")
    .awaitTermination()
)