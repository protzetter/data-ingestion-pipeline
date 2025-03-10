"""
Module for performing data quality checks on DataFrames.
"""
from loguru import logger
from pyspark.sql import functions as F


class DataValidator:
    """Class for validating data quality in Spark DataFrames."""
    
    def __init__(self, config=None):
        """
        Initialize the data validator.
        
        Args:
            config (dict): Configuration for validation rules
        """
        self.config = config or {}
        self.rules = self.config.get("rules", [])
        
    def validate(self, df):
        """
        Validate the DataFrame against configured rules.
        
        Args:
            df: Spark DataFrame to validate
            
        Returns:
            dict: Validation results with pass/fail status and details
        """
        logger.info("Starting data validation")
        
        results = {
            "passed": True,
            "total_rules": len(self.rules),
            "passed_rules": 0,
            "failures": []
        }
        
        for rule in self.rules:
            rule_type = rule.get("type")
            rule_name = rule.get("name", rule_type)
            
            try:
                if rule_type == "not_null":
                    self._check_not_null(df, rule, results)
                elif rule_type == "unique":
                    self._check_unique(df, rule, results)
                elif rule_type == "value_range":
                    self._check_value_range(df, rule, results)
                elif rule_type == "regex_match":
                    self._check_regex_match(df, rule, results)
                elif rule_type == "custom_sql":
                    self._check_custom_sql(df, rule, results)
                else:
                    logger.warning(f"Unknown rule type: {rule_type}")
                    continue
                    
                results["passed_rules"] += 1
                logger.info(f"Rule '{rule_name}' passed")
                
            except Exception as e:
                logger.error(f"Error validating rule '{rule_name}': {str(e)}")
                results["failures"].append({
                    "rule": rule_name,
                    "error": str(e)
                })
                results["passed"] = False
        
        logger.info(f"Validation complete: {results['passed_rules']}/{results['total_rules']} rules passed")
        return results
    
    def _check_not_null(self, df, rule, results):
        """Check that specified columns don't contain null values."""
        columns = rule.get("columns", [])
        threshold = rule.get("threshold", 1.0)  # Default: 100% non-null
        
        for column in columns:
            null_count = df.filter(F.col(column).isNull()).count()
            total_count = df.count()
            
            if total_count == 0:
                non_null_ratio = 1.0
            else:
                non_null_ratio = (total_count - null_count) / total_count
                
            if non_null_ratio < threshold:
                results["passed"] = False
                results["failures"].append({
                    "rule": rule.get("name", "not_null"),
                    "column": column,
                    "details": f"Non-null ratio {non_null_ratio:.2f} below threshold {threshold}"
                })
    
    def _check_unique(self, df, rule, results):
        """Check that specified columns or column combinations are unique."""
        columns = rule.get("columns", [])
        if not columns:
            return
            
        distinct_count = df.select(*columns).distinct().count()
        total_count = df.count()
        
        if distinct_count < total_count:
            results["passed"] = False
            results["failures"].append({
                "rule": rule.get("name", "unique"),
                "columns": columns,
                "details": f"Found {total_count - distinct_count} duplicate rows"
            })
    
    def _check_value_range(self, df, rule, results):
        """Check that values in specified columns are within given ranges."""
        column = rule.get("column")
        min_value = rule.get("min")
        max_value = rule.get("max")
        
        if not column:
            return
            
        query_parts = []
        if min_value is not None:
            query_parts.append(f"({column} < {min_value})")
        if max_value is not None:
            query_parts.append(f"({column} > {max_value})")
            
        if query_parts:
            query = " OR ".join(query_parts)
            out_of_range_count = df.filter(query).count()
            
            if out_of_range_count > 0:
                results["passed"] = False
                results["failures"].append({
                    "rule": rule.get("name", "value_range"),
                    "column": column,
                    "details": f"Found {out_of_range_count} values outside range"
                })
    
    def _check_regex_match(self, df, rule, results):
        """Check that values in specified columns match regex patterns."""
        column = rule.get("column")
        pattern = rule.get("pattern")
        
        if not column or not pattern:
            return
            
        non_matching_count = df.filter(~F.col(column).rlike(pattern)).count()
        
        if non_matching_count > 0:
            results["passed"] = False
            results["failures"].append({
                "rule": rule.get("name", "regex_match"),
                "column": column,
                "details": f"Found {non_matching_count} values not matching pattern"
            })
    
    def _check_custom_sql(self, df, rule, results):
        """Run custom SQL expression for validation."""
        expression = rule.get("expression")
        
        if not expression:
            return
            
        df.createOrReplaceTempView("data_to_validate")
        validation_result = self.spark.sql(f"SELECT COUNT(*) as failure_count FROM data_to_validate WHERE {expression}")
        failure_count = validation_result.collect()[0]["failure_count"]
        
        if failure_count > 0:
            results["passed"] = False
            results["failures"].append({
                "rule": rule.get("name", "custom_sql"),
                "details": f"Custom SQL validation failed with {failure_count} violations"
            })