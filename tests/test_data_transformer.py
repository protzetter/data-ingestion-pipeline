"""
Unit tests for the DataTransformer class.
"""
import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.transformation.data_transformer import DataTransformer


@pytest.fixture
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("TestDataTransformer") \
        .master("local[1]") \
        .getOrCreate()


@pytest.fixture
def sample_data(spark):
    """Create a sample DataFrame for testing."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("date", StringType(), True),
        StructField("category", StringType(), True)
    ])
    
    data = [
        Row(id="1", name="John Doe", amount=125.5, date="2023-01-15", category="A"),
        Row(id="2", name="Jane Smith", amount=750.25, date="2023-01-16", category="B"),
        Row(id="3", name="Robert Johnson", amount=45.75, date="2023-01-17", category="A"),
        Row(id="4", name="Emily Williams", amount=1200.0, date="2023-01-18", category="C")
    ]
    
    return spark.createDataFrame(data, schema)


def test_transformer_init():
    """Test DataTransformer initialization."""
    config = {"transformations": [{"type": "rename_columns", "mapping": {"id": "record_id"}}]}
    transformer = DataTransformer(config)
    
    assert transformer.config == config
    assert transformer.transformations == config["transformations"]


def test_rename_columns(spark, sample_data):
    """Test renaming columns transformation."""
    config = {
        "transformations": [
            {
                "type": "rename_columns",
                "mapping": {
                    "id": "record_id",
                    "name": "full_name"
                }
            }
        ]
    }
    
    transformer = DataTransformer(config)
    result_df = transformer.transform(sample_data)
    
    # Check that columns were renamed
    assert "record_id" in result_df.columns
    assert "full_name" in result_df.columns
    assert "id" not in result_df.columns
    assert "name" not in result_df.columns


def test_cast_columns(spark, sample_data):
    """Test casting columns transformation."""
    config = {
        "transformations": [
            {
                "type": "cast_columns",
                "casts": {
                    "amount": "integer",
                    "date": "date"
                }
            }
        ]
    }
    
    transformer = DataTransformer(config)
    result_df = transformer.transform(sample_data)
    
    # Check data types
    assert result_df.schema["amount"].dataType.simpleString() == "integer"
    assert result_df.schema["date"].dataType.simpleString() == "date"


def test_drop_columns(spark, sample_data):
    """Test dropping columns transformation."""
    config = {
        "transformations": [
            {
                "type": "drop_columns",
                "columns": ["category", "date"]
            }
        ]
    }
    
    transformer = DataTransformer(config)
    result_df = transformer.transform(sample_data)
    
    # Check that columns were dropped
    assert "category" not in result_df.columns
    assert "date" not in result_df.columns
    assert len(result_df.columns) == 3  # id, name, amount


def test_filter_rows(spark, sample_data):
    """Test filtering rows transformation."""
    config = {
        "transformations": [
            {
                "type": "filter_rows",
                "condition": "amount > 100"
            }
        ]
    }
    
    transformer = DataTransformer(config)
    result_df = transformer.transform(sample_data)
    
    # Check that rows were filtered
    assert result_df.count() == 3  # Only 3 rows have amount > 100
    assert result_df.filter("amount <= 100").count() == 0


def test_add_columns(spark, sample_data):
    """Test adding columns transformation."""
    config = {
        "transformations": [
            {
                "type": "add_columns",
                "columns": {
                    "amount_category": "CASE WHEN amount < 100 THEN 'low' WHEN amount < 1000 THEN 'medium' ELSE 'high' END",
                    "processed": "true"
                }
            }
        ]
    }
    
    transformer = DataTransformer(config)
    result_df = transformer.transform(sample_data)
    
    # Check that columns were added
    assert "amount_category" in result_df.columns
    assert "processed" in result_df.columns
    
    # Check values
    low_count = result_df.filter("amount_category = 'low'").count()
    medium_count = result_df.filter("amount_category = 'medium'").count()
    high_count = result_df.filter("amount_category = 'high'").count()
    
    assert low_count == 1  # One row has amount < 100
    assert medium_count == 2  # Two rows have 100 <= amount < 1000
    assert high_count == 1  # One row has amount >= 1000


def test_multiple_transformations(spark, sample_data):
    """Test applying multiple transformations in sequence."""
    config = {
        "transformations": [
            {
                "type": "rename_columns",
                "mapping": {"id": "record_id"}
            },
            {
                "type": "drop_columns",
                "columns": ["category"]
            },
            {
                "type": "add_columns",
                "columns": {
                    "amount_doubled": "amount * 2"
                }
            }
        ]
    }
    
    transformer = DataTransformer(config)
    result_df = transformer.transform(sample_data)
    
    # Check that all transformations were applied
    assert "record_id" in result_df.columns
    assert "id" not in result_df.columns
    assert "category" not in result_df.columns
    assert "amount_doubled" in result_df.columns
    
    # Check values
    first_row = result_df.filter("record_id = '1'").collect()[0]
    assert first_row["amount_doubled"] == first_row["amount"] * 2