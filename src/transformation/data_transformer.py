"""
Data transformation module for the data ingestion pipeline.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from loguru import logger


class DataTransformer:
    """
    Class for transforming data based on configurable transformations.
    
    Supported transformation types:
    - rename_columns: Rename columns based on a mapping
    - cast_columns: Cast columns to specified data types
    - drop_columns: Drop specified columns
    - filter_rows: Filter rows based on a condition
    - add_columns: Add new columns with expressions
    """
    
    def __init__(self, config):
        """
        Initialize the DataTransformer with configuration.
        
        Args:
            config (dict): Configuration containing transformation rules
        """
        self.config = config
        self.transformations = config.get("transformations", [])
    
    def transform(self, df):
        """
        Apply all configured transformations to the DataFrame.
        
        Args:
            df (DataFrame): The DataFrame to transform
            
        Returns:
            DataFrame: The transformed DataFrame
        """
        logger.info(f"Transforming data with {len(self.transformations)} transformations")
        
        result_df = df
        
        for transformation in self.transformations:
            transform_type = transformation.get("type")
            
            logger.info(f"Applying transformation: {transform_type}")
            
            if transform_type == "rename_columns":
                result_df = self._rename_columns(result_df, transformation)
            elif transform_type == "cast_columns":
                result_df = self._cast_columns(result_df, transformation)
            elif transform_type == "drop_columns":
                result_df = self._drop_columns(result_df, transformation)
            elif transform_type == "filter_rows":
                result_df = self._filter_rows(result_df, transformation)
            elif transform_type == "add_columns":
                result_df = self._add_columns(result_df, transformation)
            elif transform_type == "extract_coordinates":
                result_df = self._extract_coordinates(result_df, transformation)
            elif transform_type == "extract_city":
                result_df = self._extract_city(result_df, transformation)
            else:
                logger.warning(f"Unknown transformation type: {transform_type}")
        
        logger.info(f"Transformation complete: {result_df.count()} rows")
        return result_df
    
    def _rename_columns(self, df, transformation):
        """Rename columns based on a mapping."""
        mapping = transformation.get("mapping", {})
        
        for old_name, new_name in mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
            else:
                logger.warning(f"Column '{old_name}' not found for renaming")
        
        return df
    
    def _cast_columns(self, df, transformation):
        """Cast columns to specified data types."""
        casts = transformation.get("casts", {})
        
        for column, data_type in casts.items():
            if column not in df.columns:
                logger.warning(f"Column '{column}' not found for casting")
                continue
            
            try:
                if data_type == "integer":
                    df = df.withColumn(column, F.col(column).cast(IntegerType()))
                elif data_type == "double":
                    df = df.withColumn(column, F.col(column).cast(DoubleType()))
                elif data_type == "string":
                    df = df.withColumn(column, F.col(column).cast(StringType()))
                elif data_type == "boolean":
                    df = df.withColumn(column, F.col(column).cast(BooleanType()))
                elif data_type == "date":
                    df = df.withColumn(column, F.to_date(F.col(column)))
                elif data_type == "timestamp":
                    df = df.withColumn(column, F.to_timestamp(F.col(column)))
                else:
                    logger.warning(f"Unsupported data type: {data_type}")
            except Exception as e:
                logger.error(f"Error casting column '{column}' to {data_type}: {str(e)}")
        
        return df
    
    def _drop_columns(self, df, transformation):
        """Drop specified columns."""
        columns = transformation.get("columns", [])
        
        # Filter out columns that don't exist
        columns_to_drop = [col for col in columns if col in df.columns]
        
        if columns_to_drop:
            df = df.drop(*columns_to_drop)
        
        return df
    
    def _filter_rows(self, df, transformation):
        """Filter rows based on a condition."""
        condition = transformation.get("condition", "")
        
        if condition:
            try:
                df = df.filter(condition)
            except Exception as e:
                logger.error(f"Error applying filter condition '{condition}': {str(e)}")
        
        return df
    
    def _add_columns(self, df, transformation):
        """Add new columns with expressions."""
        columns = transformation.get("columns", {})
        
        for column_name, expression in columns.items():
            try:
                df = df.withColumn(column_name, F.expr(expression))
            except Exception as e:
                logger.error(f"Error adding column '{column_name}' with expression '{expression}': {str(e)}")
        
        return df
    
    def _extract_coordinates(self, df, transformation):
        """Extract latitude and longitude from a coordinates column."""
        source_column = transformation.get("source_column")
        longitude_column = transformation.get("longitude_column", "longitude")
        latitude_column = transformation.get("latitude_column", "latitude")
        
        if source_column not in df.columns:
            logger.warning(f"Source column '{source_column}' not found for coordinate extraction")
            return df
        
        try:
            # Extract coordinates from format like "[lon,lat]"
            df = df.withColumn("coordinates", F.regexp_extract(F.col(source_column), "\\[(.*?)\\]", 1))
            df = df.withColumn(longitude_column, F.split(F.col("coordinates"), ",").getItem(0).cast(DoubleType()))
            df = df.withColumn(latitude_column, F.split(F.col("coordinates"), ",").getItem(1).cast(DoubleType()))
            
            # Drop the temporary column
            df = df.drop("coordinates")
        except Exception as e:
            logger.error(f"Error extracting coordinates from '{source_column}': {str(e)}")
        
        return df
    
    def _extract_city(self, df, transformation):
        """Extract city from an address column."""
        source_column = transformation.get("source_column")
        target_column = transformation.get("target_column", "city")
        pattern = transformation.get("pattern", "\\d{5}\\s+([\\w-]+)")
        
        if source_column not in df.columns:
            logger.warning(f"Source column '{source_column}' not found for city extraction")
            return df
        
        try:
            df = df.withColumn(target_column, F.regexp_extract(F.col(source_column), pattern, 1))
        except Exception as e:
            logger.error(f"Error extracting city from '{source_column}': {str(e)}")
        
        return df