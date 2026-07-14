"""Unit tests for the silver CDC data-processing logic (dedup_latest_changes)."""
from datetime import datetime

import pytest
from pyspark.testing import assertDataFrameEqual

from silver_cdc import dedup_latest_changes

# slip_seq_id, creation_date, operation_type, amount_rp
SCHEMA = "slip_seq_id LONG, creation_date TIMESTAMP, operation_type STRING, amount_rp DOUBLE"


def _df(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


@pytest.mark.parametrize(
    "input_rows, expected_rows",
    [
        pytest.param(
            [
                (1, datetime(2026, 1, 1, 10, 0), "I", 100.0),
                (1, datetime(2026, 1, 1, 12, 0), "U", 150.0),
            ],
            [(1, datetime(2026, 1, 1, 12, 0), "U", 150.0)],
            id="keeps_latest_row_per_key",
        ),
        pytest.param(
            [
                (7, datetime(2026, 3, 1, 8, 0), "I", 10.0),
                (7, datetime(2026, 3, 1, 9, 0), "U", 20.0),
                (7, datetime(2026, 3, 1, 10, 0), "D", 20.0),
            ],
            [(7, datetime(2026, 3, 1, 10, 0), "D", 20.0)],
            id="latest_delete_wins_over_earlier_update",
        ),
        pytest.param(
            [
                (1, datetime(2026, 1, 1, 10, 0), "I", 100.0),
                (1, datetime(2026, 1, 1, 11, 0), "U", 110.0),
                (2, datetime(2026, 1, 1, 9, 0), "I", 200.0),
            ],
            [
                (1, datetime(2026, 1, 1, 11, 0), "U", 110.0),
                (2, datetime(2026, 1, 1, 9, 0), "I", 200.0),
            ],
            id="multiple_keys_collapsed_independently",
        ),
        pytest.param(
            [
                (1, datetime(2026, 1, 1, 10, 0), "I", 100.0),
                (2, datetime(2026, 1, 1, 10, 0), "I", 200.0),
                (3, datetime(2026, 1, 1, 10, 0), "I", 300.0),
            ],
            [
                (1, datetime(2026, 1, 1, 10, 0), "I", 100.0),
                (2, datetime(2026, 1, 1, 10, 0), "I", 200.0),
                (3, datetime(2026, 1, 1, 10, 0), "I", 300.0),
            ],
            id="already_unique_keys_pass_through",
        ),
        pytest.param(
            [(5, datetime(2026, 2, 1, h, 0), "U", float(h)) for h in range(1, 6)],
            [(5, datetime(2026, 2, 1, 5, 0), "U", 5.0)],
            id="collapses_many_rows_to_one_per_key",
        ),
        pytest.param(
            [],
            [],
            id="empty_input_returns_empty",
        ),
    ],
)
def test_dedup_latest_changes(spark, input_rows, expected_rows):
    result = dedup_latest_changes(_df(spark, input_rows))

    assertDataFrameEqual(result, _df(spark, expected_rows))

