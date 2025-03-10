"""
Module for reading CSV files into Spark DataFrames.
"""
from loguru import logger


class CSVReader:
    """Class for reading CSV files with configurable options."""
    
    def __init__(self, spark, config=None):
        """
        Initialize the CSV reader.
        
        Args:
            spark: SparkSession object
            config (dict): Configuration for CSV reading
        """
        self.spark = spark
        self.config = config or {}
        
    def read(self):
        """
        Read CSV file(s) into a Spark DataFrame.
        
        Returns:
            DataFrame: Spark DataFrame containing the CSV data
        """
        input_path = self.config.get("input_path")
        if not input_path:
            raise ValueError("Input path must be specified in configuration")
        
        logger.info(f"Reading CSV from {input_path}")
        
        # Build read options
        read_options = {
            "header": self.config.get("header", "true"),
            "inferSchema": self.config.get("infer_schema", "true"),
            "delimiter": self.config.get("delimiter", ","),
            "mode": self.config.get("mode", "PERMISSIVE")
        }
        
        # Add any additional options from config
        for key, value in self.config.get("options", {}).items():
            read_options[key] = value
            
        # Read the CSV file
        df = self.spark.read.format("csv").options(**read_options).load(input_path)
        
        # Log schema and row count
        logger.info(f"CSV loaded with schema: {df.schema}")
        row_count = df.count()
        logger.info(f"Read {row_count} rows from CSV")
        
        return df