import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """A lightweight local SparkSession shared across the test session.

    Requires a Java runtime and the `pyspark` package (see the dev dependency
    group). No Delta or Hive support is needed for the pure transform tests.
    """
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("albert-databricks-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
