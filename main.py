#!/usr/bin/env python3
"""
Main entry point for the data ingestion pipeline.
"""
import argparse
import yaml
from loguru import logger

from src.ingestion.csv_reader import CSVReader
from src.quality_checks.data_validator import DataValidator
from src.transformation.data_transformer import DataTransformer
from src.utils.spark_session import get_spark_session


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Data Ingestion Pipeline")
    parser.add_argument(
        "--config", 
        required=True,
        help="Path to the pipeline configuration file"
    )
    return parser.parse_args()


def load_config(config_path):
    """Load configuration from YAML file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def run_pipeline(config):
    """Execute the data ingestion pipeline."""
    logger.info("Starting data ingestion pipeline")
    
    # Initialize Spark session
    spark = get_spark_session(config.get("spark", {}))
    
    try:
        # Step 1: Ingest data
        logger.info("Ingesting data from CSV")
        reader = CSVReader(spark, config.get("ingestion", {}))
        df = reader.read()
        
        # Step 2: Perform data quality checks
        logger.info("Performing data quality checks")
        validator = DataValidator(config.get("quality_checks", {}))
        validation_results = validator.validate(df)
        
        if not validation_results["passed"]:
            logger.error(f"Data quality checks failed: {validation_results['failures']}")
            if config.get("strict_validation", False):
                raise ValueError("Data quality validation failed in strict mode")
            logger.warning("Continuing pipeline despite validation failures")
        
        # Step 3: Apply transformations
        logger.info("Applying data transformations")
        transformer = DataTransformer(config.get("transformation", {}))
        transformed_df = transformer.transform(df)
        
        # Step 4: Save the processed data
        output_path = config.get("output", {}).get("path", "data/output")
        output_format = config.get("output", {}).get("format", "parquet")
        
        logger.info(f"Saving processed data to {output_path} in {output_format} format")
        transformed_df.write.format(output_format).mode("overwrite").save(output_path)
        
        logger.info("Data ingestion pipeline completed successfully")
        
    finally:
        # Clean up resources
        spark.stop()


if __name__ == "__main__":
    args = parse_args()
    config = load_config(args.config)
    run_pipeline(config)