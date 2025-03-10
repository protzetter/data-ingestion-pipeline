"""
Pytest configuration file for the data ingestion pipeline tests.
"""
import os
import sys
import pytest
from pyspark.sql import SparkSession

# Add the project root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session that can be reused across tests."""
    spark = SparkSession.builder \
        .appName("DataIngestionPipelineTests") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    yield spark
    
    # Stop the SparkSession after all tests are done
    spark.stop()


@pytest.fixture(autouse=True)
def cleanup_spark_tables(spark):
    """Clean up any temporary tables after each test."""
    yield
    for table in spark.catalog.listTables():
        spark.catalog.dropTempView(table.name)