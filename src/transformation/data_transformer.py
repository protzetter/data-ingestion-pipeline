"""
Module for transforming data using PySpark.
"""
from loguru import logger
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class DataTransformer:
    """Class for applying transformations to Spark DataFrames."""
    
    def __init__(self, config=None):
        """
        Initialize the data transformer.
        
        Args:
            config (dict): Configuration for transformations
        """
        self.config = config or {}
        self.transformations = self.config.get("transformations", [])
        
    def transform(self, df):
        """
        Apply configured transformations to the DataFrame.
        
        Args:
            df: Spark DataFrame to transform
            
        Returns:
            DataFrame: Transformed Spark DataFrame
        """
        logger.info("Starting data transformation")
        
        transformed_df = df
        
        for transformation in self.transformations:
            transform_type = transformation.get("type")
            logger.info(f"Applying transformation: {transform_type}")
            
            try:
                if transform_type == "rename_columns":
                    transformed_df = self._rename_columns(transformed_df, transformation)
                elif transform_type == "cast_columns":
                    transformed_df = self._cast_columns(transformed_df, transformation)
                elif transform_type == "drop_columns":
                    transformed_df = self._drop_columns(transformed_df, transformation)
                elif transform_type == "filter_rows":
                    transformed_df = self._filter_rows(transformed_df, transformation)
                elif transform_type == "add_columns":
                    transformed_df = self._add_columns(transformed_df, transformation)
                elif transform_type == "aggregate":
                    transformed_df = self._aggregate(transformed_df, transformation)
                elif transform_type == "join":
                    transformed_df = self._join(transformed_df, transformation)
                else:
                    logger.warning(f"Unknown transformation type: {transform_type}")
            except Exception as e:
                logger.error(f"Error applying transformation {transform_type}: {str(e)}")
                raise
        
        logger.info("Data transformation completed")
        return transformed_df
    
    def _rename_columns(self, df, config):
        """Rename columns based on mapping."""
        mapping = config.get("mapping", {})
        for old_name, new_name in mapping.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df
    
    def _cast_columns(self, df, config):
        """Cast columns to specified data types."""
        casts = config.get("casts", {})
        for column, data_type in casts.items():
            df = df.withColumn(column, F.col(column).cast(data_type))
        return df
    
    def _drop_columns(self, df, config):
        """Drop specified columns."""
        columns = config.get("columns", [])
        return df.drop(*columns)
    
    def _filter_rows(self, df, config):
        """Filter rows based on condition."""
        condition = config.get("condition")
        if condition:
            return df.filter(condition)
        return df
    
    def _add_columns(self, df, config):
        """Add new columns based on expressions."""
        columns = config.get("columns", {})
        for column_name, expression in columns.items():
            df = df.withColumn(column_name, F.expr(expression))
        return df
    
    def _aggregate(self, df, config):
        """Perform aggregation operations."""
        group_by = config.get("group_by", [])
        aggregations = config.get("aggregations", {})
        
        if not group_by or not aggregations:
            return df
            
        agg_exprs = []
        for output_col, agg_config in aggregations.items():
            agg_type = agg_config.get("type")
            input_col = agg_config.get("column")
            
            if agg_type == "sum":
                agg_exprs.append(F.sum(input_col).alias(output_col))
            elif agg_type == "avg":
                agg_exprs.append(F.avg(input_col).alias(output_col))
            elif agg_type == "min":
                agg_exprs.append(F.min(input_col).alias(output_col))
            elif agg_type == "max":
                agg_exprs.append(F.max(input_col).alias(output_col))
            elif agg_type == "count":
                agg_exprs.append(F.count(input_col).alias(output_col))
            elif agg_type == "count_distinct":
                agg_exprs.append(F.countDistinct(input_col).alias(output_col))
        
        return df.groupBy(*group_by).agg(*agg_exprs)
    
    def _join(self, df, config):
        """Join with another DataFrame."""
        right_path = config.get("right_path")
        join_type = config.get("join_type", "inner")
        join_condition = config.get("condition")
        
        if not right_path or not join_condition:
            return df
            
        # Read the right DataFrame
        right_options = config.get("right_options", {})
        right_format = config.get("right_format", "csv")
        right_df = self.spark.read.format(right_format).options(**right_options).load(right_path)
        
        return df.join(right_df, on=join_condition, how=join_type)