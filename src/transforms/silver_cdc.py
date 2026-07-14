"""Pure, importable data-processing helpers for the silver CDC transform.

Kept free of any I/O or Spark session setup so the logic can be unit tested
against a local SparkSession without touching Delta, streaming, or argparse.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def dedup_latest_changes(
    microbatch_df: DataFrame,
    keys: list[str],
    sequence_col: str,
) -> DataFrame:
    """Collapse a CDC microbatch to a single latest row per key.

    A Delta MERGE requires at most one source row per key. Within each key group
    we keep only the row with the greatest ``sequence_col`` (latest change wins).

    Args:
        microbatch_df: incoming change rows.
        keys: columns that identify a row (the merge key).
        sequence_col: ordering column; the max value per key is retained.

    Returns:
        A DataFrame with the same schema as the input and at most one row per key.
    """
    dedup_window = Window.partitionBy(*keys).orderBy(F.col(sequence_col).desc())
    return (
        microbatch_df
        .withColumn("_change_rank", F.row_number().over(dedup_window))
        .filter("_change_rank = 1")
        .drop("_change_rank")
    )
