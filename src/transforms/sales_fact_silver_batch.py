"""Silver CDC transform, solution #3: plain batch with a self-managed watermark.

Unlike the streaming version (checkpoint-managed offsets) and the DLT version
(engine-managed state), this job owns its incremental bookkeeping explicitly:

  1. Read the last processed watermark from a small Delta control table.
  2. Read the bronze source as a *batch* snapshot, keeping only rows newer than
     that watermark (using the ingestion timestamp added at ingest time).
  3. Collapse to the latest change per key and MERGE (I/U/D) into silver.
  4. Persist the new watermark ONLY after a successful merge.

Because the MERGE is idempotent (keyed upserts with a sequence guard, and
no-op deletes for already-absent rows), advancing the watermark after the merge
gives safe at-least-once semantics: a crash between the merge and the watermark
write simply reprocesses the same changes on the next run with no ill effect.
"""
import argparse
import logging
import os
import sys
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Make sibling modules importable regardless of how the file is launched.
#sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from silver_cdc import dedup_latest_changes

spark = SparkSession.builder.getOrCreate()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("apply_changes_batch")

parser = argparse.ArgumentParser()
parser.add_argument("--source_layer")
parser.add_argument("--target_layer")
parser.add_argument("--checkpoints_catalog")
parser.add_argument("--table_schema")
parser.add_argument("--source_table_name")
parser.add_argument("--target_table_name")
parser.add_argument("--watermark_column", default="_INGESTION_TIME")
args = parser.parse_args()

source_layer = args.source_layer
target_layer = args.target_layer
checkpoints_catalog = args.checkpoints_catalog
table_schema = args.table_schema
source_table_name = args.source_table_name
target_table_name = args.target_table_name
watermark_column = args.watermark_column


source_table = f"{source_layer}.{table_schema}.{source_table_name}"
target_table = f"{target_layer}.{table_schema}.{target_table_name}"
watermark_table = f"{checkpoints_catalog}.{table_schema}.cdc_watermarks"

# Epoch sentinel used on the very first run (before any watermark is stored).
EPOCH = datetime(1970, 1, 1)

# Create the silver target as an empty managed Delta table with an explicit
# schema (a curated contract) on first run.
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
    .clusterBy("slip_seq_id")
    .property("delta.autoOptimize.optimizeWrite", "true")
    .property("delta.autoOptimize.autoCompact", "true")
    .execute()
)

# Create the watermark control table (one row per target table).
(
    DeltaTable.createIfNotExists(spark)
    .tableName(watermark_table)
    .addColumn("target_table", "STRING")
    .addColumn("watermark", "TIMESTAMP")
    .execute()
)

# 1. Read the last processed watermark for this target.
wm_rows = (
    spark.table(watermark_table)
    .filter(F.col("target_table") == target_table)
    .select("watermark")
    .collect()
)
last_watermark = wm_rows[0]["watermark"] if wm_rows else EPOCH
logger.info("Last watermark for %s: %s", target_table, last_watermark)

# 2. Read bronze as a batch snapshot, keeping only rows newer than the watermark.
source_df = spark.read.table(source_table)
new_changes = source_df.filter(F.col(watermark_column) > F.lit(last_watermark))

# Diagnostic counts: if `new` == `total` on every run, the watermark is not
# being persisted/read (last_watermark stuck at epoch); if `new` shrinks to 0
# on the next run, incremental filtering is working as intended.
logger.info(
    "Source %s: %d total row(s), %d newer than watermark",
    source_table,
    source_df.count(),
    new_changes.count(),
)

# The new high-watermark is the max ingestion time in this snapshot. If it is
# NULL there were no new rows, so we exit without touching silver or the wm.
new_watermark = new_changes.agg(F.max(watermark_column).alias("wm")).collect()[0]["wm"]
if new_watermark is None:
    logger.info("No new changes since %s, skipping merge", last_watermark)
else:

    # 3. Collapse to the latest change per key (reuses the unit-tested helper) and
    #    MERGE the I/U/D operations into silver.
    latest_changes = dedup_latest_changes(
        new_changes,
        keys=["slip_seq_id"],
        sequence_col="creation_date"
        )
    column_map = {c: f"s.{c}" for c in spark.table(target_table).columns}

    (
        DeltaTable.forName(spark, target_table)
        .alias("t")
        .merge(latest_changes.alias("s"), "t.slip_seq_id = s.slip_seq_id")
        .whenMatchedDelete(condition="s.operation_type = 'D'")
        .whenMatchedUpdate(
            condition="s.operation_type = 'U' AND s.creation_date >= t.creation_date",
            set=column_map,
        )
        .whenNotMatchedInsert(condition="s.operation_type = 'I'", values=column_map)
        .execute()
    )
    logger.info("Merged changes into %s (new watermark: %s)", target_table, new_watermark)

    # 4. Persist the new watermark only after the merge succeeded.
    wm_update = spark.createDataFrame(
        [(target_table, new_watermark)],
        "target_table STRING, watermark TIMESTAMP",
    )

    (
        DeltaTable.forName(spark, watermark_table)
        .alias("w")
        .merge(wm_update.alias("n"), "w.target_table = n.target_table")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info("Watermark for %s advanced to %s", target_table, new_watermark)
