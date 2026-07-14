import dlt
from pyspark.sql import functions as F

# DLT pipelines are configured declaratively: values come from the pipeline
# `configuration` block (spark.conf) instead of argparse. The DLT runtime
# manages the streaming checkpoint, target creation, and incremental state,
# so none of that plumbing lives here.
source_layer = spark.conf.get("source_layer")
table_schema = spark.conf.get("table_schema")
# Source and target names are decoupled: DLT reads the existing bronze source
# and writes to a separately named target it fully owns.
source_table_name = spark.conf.get("source_table_name")
target_table_name = spark.conf.get("target_table_name")

source_table = f"{source_layer}.{table_schema}.{source_table_name}"


# Streaming view over the bronze change feed. Selecting + casting here enforces
# the curated schema contract (types and lowercase names) that the standalone
# script previously encoded in DeltaTable.createIfNotExists.
@dlt.view(name="sales_fact_changes")
def sales_fact_changes():
    return (
        spark.readStream.table(source_table).select(
            F.col("slip_seq_id").cast("bigint"),
            F.col("creation_date").cast("timestamp"),
            F.col("site_id").cast("int"),
            F.col("start_date").cast("timestamp"),
            F.col("end_date").cast("timestamp"),
            F.col("duration").cast("int"),
            F.col("item_count").cast("int"),
            F.col("amount_rp").cast("decimal(18,2)"),
            F.col("line_count").cast("int"),
            F.col("_INGESTION_TIME").cast("timestamp").alias("_ingestion_time"),
            F.col("_SOURCE_FILE").cast("string").alias("_source_file"),
            F.col("operation_type"),
        )
    )


# Declare the curated target streaming table. Liquid clustering on the merge key
# and auto-optimize properties mirror the original createIfNotExists contract.
# The DLT expectations replace what you'd otherwise assert with Great Expectations.
dlt.create_streaming_table(
    name=target_table_name,
    comment="Curated sales fact (silver), CDC-applied from bronze via DLT AUTO CDC.",
    cluster_by=["slip_seq_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
    expect_all_or_drop={
        "valid_key": "slip_seq_id IS NOT NULL",
    },
)


# The declarative equivalent of the whenMatchedDelete / whenMatchedUpdate /
# whenNotMatchedInsert merge:
#   - keys              -> the MERGE join condition (t.slip_seq_id = s.slip_seq_id)
#   - sequence_by       -> replaces the row_number dedup AND the
#                          "s.creation_date >= t.creation_date" out-of-order guard
#   - apply_as_deletes  -> the whenMatchedDelete(operation_type = 'D')
#   - except_column_list-> drops operation_type so it is not persisted to silver
#   - stored_as_scd_type=1 -> overwrite-in-place upserts (no history), matching
#                             the original insert/update behaviour
dlt.apply_changes(
    target=target_table_name,
    source="sales_fact_changes",
    keys=["slip_seq_id"],
    sequence_by=F.col("creation_date"),
    apply_as_deletes=F.expr("operation_type = 'D'"),
    except_column_list=["operation_type"],
    stored_as_scd_type=1,
)
