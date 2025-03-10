#!/usr/bin/env python3
"""
Data ingestion pipeline for electric vehicle charging stations data.
This script processes the Electra charging stations CSV file.
"""
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DoubleType
from loguru import logger

# Initialize Spark session
def get_spark_session(app_name="ElectraChargingStationsPipeline", master="local[*]"):
    """Create and configure a Spark session."""
    return SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

# Define schema for the CSV file
def get_schema():
    """Define the schema for the Electra charging stations CSV."""
    return StructType([
        StructField("id_pdc_itinerance", StringType(), True),
        StructField("observations", StringType(), True),
        StructField("prise_type_2", BooleanType(), True),
        StructField("prise_type_autre", BooleanType(), True),
        StructField("prise_type_chademo", BooleanType(), True),
        StructField("prise_type_combo_ccs", BooleanType(), True),
        StructField("prise_type_ef", BooleanType(), True),
        StructField("puissance_nominale", IntegerType(), True),
        StructField("nom_amenageur", StringType(), True),
        StructField("siren_amenageur", StringType(), True),
        StructField("contact_amenageur", StringType(), True),
        StructField("nom_operateur", StringType(), True),
        StructField("contact_operateur", StringType(), True),
        StructField("nom_enseigne", StringType(), True),
        StructField("id_station_itinerance", StringType(), True),
        StructField("nom_station", StringType(), True),
        StructField("implantation_station", StringType(), True),
        StructField("adresse_station", StringType(), True),
        StructField("coordonneesXY", StringType(), True),
        StructField("nbre_pdc", IntegerType(), True),
        StructField("gratuit", BooleanType(), True),
        StructField("paiement_acte", BooleanType(), True),
        StructField("paiement_cb", BooleanType(), True),
        StructField("paiement_autre", BooleanType(), True),
        StructField("condition_acces", StringType(), True),
        StructField("reservation", BooleanType(), True),
        StructField("horaires", StringType(), True),
        StructField("accessibilite_pmr", StringType(), True),
        StructField("restriction_gabarit", StringType(), True),
        StructField("station_deux_roues", BooleanType(), True),
        StructField("raccordement", StringType(), True),
        StructField("num_pdl", StringType(), True),
        StructField("date_mise_en_service", StringType(), True),
        StructField("date_maj", StringType(), True)
    ])

# Read CSV file
def read_electra_csv(spark, input_path):
    """Read Electra charging stations CSV file into a Spark DataFrame."""
    logger.info(f"Reading CSV from {input_path}")
    
    options = {
        "header": "true",
        "delimiter": ",",
        "mode": "PERMISSIVE",
        "nullValue": "N/A"
    }
    
    # Read with schema
    schema = get_schema()
    df = spark.read.format("csv") \
        .options(**options) \
        .schema(schema) \
        .load(input_path)
    
    logger.info(f"CSV loaded with {df.count()} rows")
    return df

# Perform data quality checks
def validate_data(df):
    """Perform data quality checks on the Electra charging stations data."""
    logger.info("Performing data quality checks")
    
    # Check for null values in key fields
    key_fields = ["id_pdc_itinerance", "nom_station", "adresse_station", "puissance_nominale"]
    null_counts = {}
    
    for field in key_fields:
        null_count = df.filter(F.col(field).isNull()).count()
        null_counts[field] = null_count
        if null_count > 0:
            logger.warning(f"Found {null_count} null values in column '{field}'")
    
    # Check for duplicate IDs
    total_rows = df.count()
    distinct_ids = df.select("id_pdc_itinerance").distinct().count()
    if distinct_ids < total_rows:
        logger.warning(f"ID column is not unique: {total_rows - distinct_ids} duplicates found")
    
    # Check date format
    invalid_dates = df.filter(~F.col("date_mise_en_service").rlike("^\\d{4}-\\d{2}-\\d{2}$")).count()
    if invalid_dates > 0:
        logger.warning(f"Found {invalid_dates} rows with invalid date_mise_en_service format")
    
    return {
        "null_counts": null_counts,
        "duplicate_ids": total_rows - distinct_ids,
        "invalid_dates": invalid_dates
    }

# Transform data
def transform_data(df):
    """Apply transformations to the Electra charging stations data."""
    logger.info("Applying data transformations")
    
    # Extract latitude and longitude from coordonneesXY
    df = df.withColumn("coordinates", F.regexp_extract(F.col("coordonneesXY"), "\\[(.*?)\\]", 1))
    df = df.withColumn("longitude", F.split(F.col("coordinates"), ",").getItem(0).cast(DoubleType()))
    df = df.withColumn("latitude", F.split(F.col("coordinates"), ",").getItem(1).cast(DoubleType()))
    
    # Convert date strings to date type
    df = df.withColumn("date_mise_en_service", F.to_date(F.col("date_mise_en_service"), "yyyy-MM-dd"))
    df = df.withColumn("date_maj", F.to_date(F.col("date_maj"), "yyyy-MM-dd"))
    
    # Create a charging_type column based on available plug types
    df = df.withColumn(
        "charging_type",
        F.when(F.col("prise_type_combo_ccs"), "CCS")
         .when(F.col("prise_type_chademo"), "CHAdeMO")
         .when(F.col("prise_type_2"), "Type 2")
         .when(F.col("prise_type_ef"), "EF")
         .when(F.col("prise_type_autre"), "Other")
         .otherwise("Unknown")
    )
    
    # Create a power_category column
    df = df.withColumn(
        "power_category",
        F.when(F.col("puissance_nominale") < 50, "Low")
         .when(F.col("puissance_nominale") < 150, "Medium")
         .when(F.col("puissance_nominale") >= 150, "High")
         .otherwise("Unknown")
    )
    
    # Add a processed_timestamp column
    df = df.withColumn("processed_timestamp", F.current_timestamp())
    
    # Extract city from address
    df = df.withColumn(
        "city", 
        F.regexp_extract(F.col("adresse_station"), "\\d{5}\\s+([\\w-]+)", 1)
    )
    
    # Select and reorder columns for final output
    columns_to_select = [
        "id_pdc_itinerance", 
        "nom_station", 
        "adresse_station",
        "city",
        "latitude", 
        "longitude", 
        "puissance_nominale", 
        "power_category",
        "charging_type",
        "nbre_pdc",
        "gratuit", 
        "horaires",
        "accessibilite_pmr",
        "date_mise_en_service",
        "date_maj",
        "processed_timestamp"
    ]
    
    df_final = df.select(columns_to_select)
    
    logger.info(f"Transformation complete: {df_final.count()} rows")
    return df_final

# Save the processed data
def save_data(df, output_path, format="parquet"):
    """Save the DataFrame to the specified path."""
    logger.info(f"Saving data to {output_path} in {format} format")
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save the data
    df.write.format(format).mode("overwrite").save(output_path)
    logger.info("Data saved successfully")

# Main function
def main():
    """Run the data ingestion pipeline for Electra charging stations."""
    logger.info("Starting Electra charging stations data ingestion pipeline")
    
    # Initialize Spark
    spark = get_spark_session()
    
    try:
        # Read data
        input_path = "/Users/patrickrotzetter/dev/data_ingestion_pipeline/data/raw/bornes-de-recharge-pour-vehicules-electriques-irve-electra.csv"
        df = read_electra_csv(spark, input_path)
        
        # Show sample data
        logger.info("Sample data:")
        df.show(5, truncate=False)
        
        # Validate data
        validation_results = validate_data(df)
        logger.info(f"Validation results: {validation_results}")
        
        # Transform data
        transformed_df = transform_data(df)
        
        # Show transformed data
        logger.info("Transformed data:")
        transformed_df.show(5, truncate=False)
        
        # Save data
        output_path = "/Users/patrickrotzetter/dev/data_ingestion_pipeline/data/output/electra_processed"
        save_data(transformed_df, output_path)
        
        # Generate some analytics
        logger.info("Generating analytics...")
        
        # Count by power category
        logger.info("Charging stations by power category:")
        transformed_df.groupBy("power_category").count().show()
        
        # Count by charging type
        logger.info("Charging stations by charging type:")
        transformed_df.groupBy("charging_type").count().show()
        
        # Count by city
        logger.info("Top 10 cities by number of charging stations:")
        transformed_df.groupBy("city").count().orderBy(F.desc("count")).show(10)
        
        logger.info("Pipeline completed successfully")
        
    finally:
        # Clean up
        spark.stop()

if __name__ == "__main__":
    main()