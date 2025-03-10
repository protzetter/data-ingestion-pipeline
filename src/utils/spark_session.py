"""
Utility module for creating and configuring Spark sessions.
"""
from pyspark.sql import SparkSession


def get_spark_session(config=None):
    """
    Create and configure a Spark session based on provided configuration.
    
    Args:
        config (dict): Configuration parameters for Spark session
        
    Returns:
        SparkSession: Configured Spark session
    """
    if config is None:
        config = {}
    
    # Start building the Spark session
    builder = SparkSession.builder.appName(config.get("app_name", "DataIngestionPipeline"))
    
    # Set master if provided
    if "master" in config:
        builder = builder.master(config["master"])
    
    # Add any additional configurations
    for key, value in config.get("conf", {}).items():
        builder = builder.config(key, value)
    
    # Create the session
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel(config.get("log_level", "WARN"))
    
    return spark