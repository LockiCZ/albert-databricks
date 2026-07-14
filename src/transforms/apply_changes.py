import argparse

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--source_layer")
parser.add_argument("--target_layer")
parser.add_argument("--checkpoint_container")
parser.add_argument("--table_schema")
parser.add_argument("--source_table_name")
parser.add_argument("--target_table_name")
args = parser.parse_args()

source_layer         = args.source_layer
target_layer         = args.target_layer
checkpoint_container = args.checkpoint_container
table_schema         = args.table_schema
source_table_name    = args.source_table_name
target_table_name    = args.target_table_name

source_table = f"{source_layer}.{table_schema}.{source_table_name}"
target_table  = f"{target_layer}.{table_schema}.{target_table_name}"
checkpoint = f"{checkpoint_container}/{target_layer}/{table_schema}/{target_table_name}"

# Business columns are everything in the change feed except the CDC operation flag.
column_map = {c: f"s.{c}" for c in spark.table(source_table).columns if c != "OPERATION_TYPE"}

# Create the silver target as an empty managed Delta table with an explicit
# schema (a curated contract) on first run. It must exist before the first
# MERGE runs inside foreachBatch.
(
    DeltaTable.createIfNotExists(spark)
    .tableName(target_table)
    .addColumn("SLIP_SEQ_ID", "BIGINT", nullable=False)
    .addColumn("CREATION_DATE", "TIMESTAMP")
    .addColumn("SITE_ID", "INT")
    .addColumn("START_DATE", "TIMESTAMP")
    .addColumn("END_DATE", "TIMESTAMP")
    .addColumn("DURATION", "INT")
    .addColumn("ITEM_COUNT", "INT")
    .addColumn("AMOUNT_RP", "DECIMAL(18,2)")
    .addColumn("LINE_COUNT", "INT")
    .execute()
)


def upsert_to_silver(microbatch_df, batch_id):
    # A single MERGE requires at most one source row per key, so collapse this
    # microbatch to the latest operation per key by sequence value.
    dedup_window = Window.partitionBy("SLIP_SEQ_ID").orderBy(F.col("CREATION_DATE").desc())
    latest_changes = (
        microbatch_df
        .withColumn("_change_rank", F.row_number().over(dedup_window))
        .filter("_change_rank = 1")
        .drop("_change_rank")
    )

    (
        DeltaTable.forName(spark, target_table)
        .alias("t")
        .merge(latest_changes.alias("s"), "t.SLIP_SEQ_ID = s.SLIP_SEQ_ID")
        .whenMatchedDelete(condition="s.OPERATION_TYPE = 'D'")
        .whenMatchedUpdate(condition="s.OPERATION_TYPE = 'U'", set=column_map)
        .whenNotMatchedInsert(condition="s.OPERATION_TYPE = 'I'", values=column_map)
        .execute()
    )


# Stream new change rows from the bronze Delta table. The checkpoint tracks which
# rows have already been processed, so each run only applies changes that arrived
# since the previous run (incremental, exactly-once).
(
    spark.readStream
    .format("delta")
    .table(source_table)
    .writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)
