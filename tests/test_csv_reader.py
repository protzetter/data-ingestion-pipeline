"""
Unit tests for the CSVReader class.
"""
import pytest
from pytest import fixture, mark  # Import specific items if you know you'll need them

from pyspark.sql import SparkSession
from src.ingestion.csv_reader import CSVReader


@pytest.fixture
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("TestCSVReader") \
        .master("local[1]") \
        .getOrCreate()


@pytest.fixture
def sample_csv(spark, tmp_path):
    """Create a sample CSV file for testing."""
    csv_path = tmp_path / "sample.csv"
    with open(csv_path, "w") as f:
        f.write("id,name,amount,date\n")
        f.write("1,John Doe,100.5,2023-01-01\n")
        f.write("2,Jane Smith,250.75,2023-01-02\n")
    
    return str(csv_path)


def test_csv_reader_init(spark):
    """Test CSVReader initialization."""
    config = {"input_path": "some/path.csv"}
    reader = CSVReader(spark, config)
    
    assert reader.spark == spark
    assert reader.config == config


def test_csv_reader_read(spark, sample_csv):
    """Test reading a CSV file."""
    config = {
        "input_path": sample_csv,
        "header": "true",
        "infer_schema": "true"
    }
    
    reader = CSVReader(spark, config)
    df = reader.read()
    
    # Check that the DataFrame has the expected structure
    assert df.count() == 2
    assert len(df.columns) == 4
    assert df.columns == ["id", "name", "amount", "date"]
    
    # Check data content
    rows = df.collect()
    assert rows[0]["name"] == "John Doe"
    assert rows[1]["name"] == "Jane Smith"


def test_csv_reader_missing_path(spark):
    """Test error when input path is missing."""
    reader = CSVReader(spark, {})
    
    with pytest.raises(ValueError, match="Input path must be specified"):
        reader.read()


def test_csv_reader_custom_options(spark, sample_csv):
    """Test reading with custom options."""
    config = {
        "input_path": sample_csv,
        "header": "true",
        "delimiter": ",",
        "options": {
            "nullValue": "NULL",
            "dateFormat": "yyyy-MM-dd"
        }
    }
    
    reader = CSVReader(spark, config)
    df = reader.read()
    
    assert df.count() == 2