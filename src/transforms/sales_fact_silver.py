import argparse
import logging

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("apply_changes")

parser = argparse.ArgumentParser()
parser.add_argument("--source_layer")
parser.add_argument("--target_layer")
parser.add_argument("--checkpoint_container")
parser.add_argument("--table_schema")
parser.add_argument("--table_name")
args = parser.parse_args()

source_layer         = args.source_layer
target_layer         = args.target_layer
checkpoint_container = args.checkpoint_container
table_schema         = args.table_schema
table_name           = args.table_name

source_table = f"{source_layer}.{table_schema}.{table_name}"
target_table = f"{target_layer}.{table_schema}.{table_name}"
checkpoint = f"{checkpoint_container}/{target_layer}/{table_schema}/{table_name}"

# Create the silver target as an empty managed Delta table with an explicit
# schema (a curated contract) on first run. It must exist before the first
# MERGE runs inside foreachBatch.
(
    DeltaTable.createIfNotExists(spark)
    .tableName(target_table)
    .addColumn("slip_seq_id", "BIGINT", nullable=False)
    .addColumn("creation_date", "TIMESTAMP")
    .addColumn("site_id", "INT")
    .addColumn("start_date", "TIMESTAMP")
    .addColumn("end_date", "TIMESTAMP")
    .addColumn("duration", "INT")
    .addColumn("item_count", "INT")
    .addColumn("amount_rp", "DECIMAL(18,2)")
    .addColumn("line_count", "INT")
    .addColumn("_ingestion_time", "TIMESTAMP")
    .addColumn("_source_file", "STRING")
    .clusterBy("slip_seq_id")          # liquid clustering on the merge key
    .property("delta.autoOptimize.optimizeWrite", "true")
    .property("delta.autoOptimize.autoCompact", "true")
    .execute()
)

column_map = {c: f"s.{c}" for c in spark.table(target_table).columns}

def upsert_to_silver(microbatch_df, batch_id):
    # A single MERGE requires at most one source row per key, so collapse this
    # microbatch to the latest operation per key by sequence value.
    dedup_window = Window.partitionBy("slip_seq_id").orderBy(F.col("creation_date").desc())
    latest_changes = (
        microbatch_df
        .withColumn("_change_rank", F.row_number().over(dedup_window))
        .filter("_change_rank = 1")
        .drop("_change_rank")
    )

    # Get operation counts for detailed logging
    counts = {
        row["operation_type"]: row["cnt"]
        for row in (
            latest_changes.groupBy("operation_type")
            .agg(F.count("*").alias("cnt"))
            .collect()
        )
    }
    total = sum(counts.values())

    if total == 0:
        print(f"batch {batch_id}: no changes, skipping merge")
        return

    print(f"batch {batch_id}: processing {total} change(s)")

    (
        DeltaTable.forName(spark, target_table)
        .alias("t")
        .merge(latest_changes.alias("s"), "t.slip_seq_id = s.slip_seq_id")
        .whenMatchedDelete(condition="s.operation_type = 'D'")
        .whenMatchedUpdate(condition="s.operation_type = 'U' AND s.creation_date >= t.creation_date", set=column_map)
        .whenNotMatchedInsert(condition="s.operation_type = 'I'", values=column_map)
        .execute()
    )

    print(
        f"batch {batch_id}: applied {total} change(s) "
        f"(inserts={counts.get('I', 0)}, updates={counts.get('U', 0)}, deletes={counts.get('D', 0)})"
    )


# Stream new change rows from the bronze Delta table. The checkpoint tracks which
# rows have already been processed, so each run only applies changes that arrived
# since the previous run (incremental, exactly-once).
logger.info("Starting streaming read from %s (availableNow mode)", source_table)
query = (
    spark.readStream
    .format("delta")
    .table(source_table)
    .writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
logger.info("Transform completed: %s", target_table)
