"""
Unit tests for the DataValidator class.
"""
import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.quality_checks.data_validator import DataValidator


@pytest.fixture
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("TestDataValidator") \
        .master("local[1]") \
        .getOrCreate()


@pytest.fixture
def sample_data(spark):
    """Create a sample DataFrame for testing."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("date", StringType(), True)
    ])
    
    data = [
        Row(id="1", name="John", amount=100, date="2023-01-01"),
        Row(id="2", name="Jane", amount=200, date="2023-01-02"),
        Row(id="3", name=None, amount=300, date="2023-01-03"),
        Row(id="4", name="Bob", amount=None, date="invalid-date"),
        Row(id="5", name="Alice", amount=500, date=None)
    ]
    
    return spark.createDataFrame(data, schema)


def test_validator_init():
    """Test DataValidator initialization."""
    config = {"rules": [{"type": "not_null", "columns": ["id"]}]}
    validator = DataValidator(config)
    
    assert validator.config == config
    assert validator.rules == config["rules"]


def test_validate_not_null(spark, sample_data):
    """Test not_null validation rule."""
    config = {
        "rules": [
            {
                "name": "check_required_fields",
                "type": "not_null",
                "columns": ["id", "name"]
            }
        ]
    }
    
    validator = DataValidator(config)
    results = validator.validate(sample_data)
    
    assert results["passed"] is False
    assert results["total_rules"] == 1
    assert results["passed_rules"] == 0
    assert len(results["failures"]) == 1
    assert "name" in results["failures"][0]["details"]


def test_validate_unique(spark, sample_data):
    """Test unique validation rule."""
    # Add a duplicate ID to test uniqueness validation
    data_with_duplicate = sample_data.union(
        spark.createDataFrame([Row(id="1", name="Duplicate", amount=600, date="2023-01-06")])
    )
    
    config = {
        "rules": [
            {
                "name": "check_unique_id",
                "type": "unique",
                "columns": ["id"]
            }
        ]
    }
    
    validator = DataValidator(config)
    results = validator.validate(data_with_duplicate)
    
    assert results["passed"] is False
    assert "duplicate" in results["failures"][0]["details"].lower()


def test_validate_value_range(spark, sample_data):
    """Test value_range validation rule."""
    config = {
        "rules": [
            {
                "name": "check_amount_range",
                "type": "value_range",
                "column": "amount",
                "min": 150,
                "max": 400
            }
        ]
    }
    
    validator = DataValidator(config)
    results = validator.validate(sample_data)
    
    assert results["passed"] is False
    assert "outside range" in results["failures"][0]["details"]


def test_validate_regex_match(spark, sample_data):
    """Test regex_match validation rule."""
    config = {
        "rules": [
            {
                "name": "check_date_format",
                "type": "regex_match",
                "column": "date",
                "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
            }
        ]
    }
    
    validator = DataValidator(config)
    results = validator.validate(sample_data)
    
    assert results["passed"] is False
    assert "not matching pattern" in results["failures"][0]["details"]


def test_validate_multiple_rules(spark, sample_data):
    """Test multiple validation rules together."""
    config = {
        "rules": [
            {
                "name": "check_id_not_null",
                "type": "not_null",
                "columns": ["id"]
            },
            {
                "name": "check_date_format",
                "type": "regex_match",
                "column": "date",
                "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
            }
        ]
    }
    
    validator = DataValidator(config)
    results = validator.validate(sample_data)
    
    assert results["passed"] is False
    assert results["total_rules"] == 2
    assert results["passed_rules"] == 1  # id check should pass
    assert len(results["failures"]) == 1  # date check should fail