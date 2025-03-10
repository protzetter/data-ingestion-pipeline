"""
Data quality validation module for the data ingestion pipeline.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from loguru import logger


class DataValidator:
    """
    Class for validating data quality based on configurable rules.
    
    Supported validation types:
    - not_null: Check if specified columns have null values
    - unique: Check if specified columns have unique values
    - value_range: Check if values in a column are within a specified range
    - regex_match: Check if values in a column match a specified regex pattern
    """
    
    def __init__(self, config):
        """
        Initialize the DataValidator with configuration.
        
        Args:
            config (dict): Configuration containing validation rules
        """
        self.config = config
        self.rules = config.get("rules", [])
    
    def validate(self, df):
        """
        Validate the DataFrame against all configured rules.
        
        Args:
            df (DataFrame): The DataFrame to validate
            
        Returns:
            dict: Validation results including passed/failed status and details
        """
        logger.info(f"Validating data with {len(self.rules)} rules")
        
        results = {
            "passed": True,
            "total_rules": len(self.rules),
            "passed_rules": 0,
            "failures": []
        }
        
        for rule in self.rules:
            rule_type = rule.get("type")
            rule_name = rule.get("name", rule_type)
            
            logger.info(f"Applying rule: {rule_name} ({rule_type})")
            
            if rule_type == "not_null":
                self._validate_not_null(df, rule, results)
            elif rule_type == "unique":
                self._validate_unique(df, rule, results)
            elif rule_type == "value_range":
                self._validate_value_range(df, rule, results)
            elif rule_type == "regex_match":
                self._validate_regex_match(df, rule, results)
            else:
                logger.warning(f"Unknown validation rule type: {rule_type}")
        
        results["passed"] = len(results["failures"]) == 0
        results["passed_rules"] = results["total_rules"] - len(results["failures"])
        
        logger.info(f"Validation complete: {results['passed_rules']}/{results['total_rules']} rules passed")
        
        return results
    
    def _validate_not_null(self, df, rule, results):
        """Validate that specified columns do not have null values."""
        columns = rule.get("columns", [])
        threshold = rule.get("threshold", 1.0)  # Default: require 100% non-null
        
        null_counts = {}
        total_rows = df.count()
        
        for column in columns:
            if column not in df.columns:
                results["failures"].append({
                    "rule": rule.get("name", "not_null"),
                    "details": f"Column '{column}' not found in DataFrame"
                })
                continue
                
            null_count = df.filter(F.col(column).isNull()).count()
            null_ratio = null_count / total_rows if total_rows > 0 else 0
            
            if null_count > 0:
                null_counts[column] = null_count
                
                # Check if null ratio exceeds threshold
                if null_ratio > (1 - threshold):
                    results["failures"].append({
                        "rule": rule.get("name", "not_null"),
                        "details": f"Column '{column}' has {null_count} null values ({null_ratio:.2%}), exceeding threshold"
                    })
        
        if not null_counts:
            logger.info(f"Rule '{rule.get('name', 'not_null')}' passed: No null values found")
    
    def _validate_unique(self, df, rule, results):
        """Validate that specified columns have unique values."""
        columns = rule.get("columns", [])
        
        for column in columns:
            if column not in df.columns:
                results["failures"].append({
                    "rule": rule.get("name", "unique"),
                    "details": f"Column '{column}' not found in DataFrame"
                })
                continue
                
            total_rows = df.count()
            distinct_count = df.select(column).distinct().count()
            
            if distinct_count < total_rows:
                duplicate_count = total_rows - distinct_count
                results["failures"].append({
                    "rule": rule.get("name", "unique"),
                    "details": f"Column '{column}' has {duplicate_count} duplicate values"
                })
                
                # Log some examples of duplicates
                duplicates = df.groupBy(column).count().filter(F.col("count") > 1)
                logger.info(f"Examples of duplicate values in '{column}':")
                duplicates.show(5, truncate=False)
            else:
                logger.info(f"Rule '{rule.get('name', 'unique')}' passed: All values in '{column}' are unique")
    
    def _validate_value_range(self, df, rule, results):
        """Validate that values in a column are within a specified range."""
        column = rule.get("column")
        min_val = rule.get("min")
        max_val = rule.get("max")
        
        if column not in df.columns:
            results["failures"].append({
                "rule": rule.get("name", "value_range"),
                "details": f"Column '{column}' not found in DataFrame"
            })
            return
        
        # Count values outside the range
        outside_range_count = 0
        
        if min_val is not None and max_val is not None:
            outside_range_count = df.filter(
                (F.col(column) < min_val) | (F.col(column) > max_val)
            ).count()
        elif min_val is not None:
            outside_range_count = df.filter(F.col(column) < min_val).count()
        elif max_val is not None:
            outside_range_count = df.filter(F.col(column) > max_val).count()
        
        if outside_range_count > 0:
            results["failures"].append({
                "rule": rule.get("name", "value_range"),
                "details": f"Column '{column}' has {outside_range_count} values outside range [{min_val}, {max_val}]"
            })
        else:
            logger.info(f"Rule '{rule.get('name', 'value_range')}' passed: All values in '{column}' are within range")
    
    def _validate_regex_match(self, df, rule, results):
        """Validate that values in a column match a specified regex pattern."""
        column = rule.get("column")
        pattern = rule.get("pattern")
        
        if column not in df.columns:
            results["failures"].append({
                "rule": rule.get("name", "regex_match"),
                "details": f"Column '{column}' not found in DataFrame"
            })
            return
        
        # Count values not matching the pattern
        non_matching_count = df.filter(
            ~F.col(column).rlike(pattern) & ~F.col(column).isNull()
        ).count()
        
        if non_matching_count > 0:
            results["failures"].append({
                "rule": rule.get("name", "regex_match"),
                "details": f"Column '{column}' has {non_matching_count} values not matching pattern '{pattern}'"
            })
            
            # Log some examples of non-matching values
            non_matching = df.filter(~F.col(column).rlike(pattern) & ~F.col(column).isNull())
            logger.info(f"Examples of values not matching pattern in '{column}':")
            non_matching.select(column).show(5, truncate=False)
        else:
            logger.info(f"Rule '{rule.get('name', 'regex_match')}' passed: All values in '{column}' match the pattern")