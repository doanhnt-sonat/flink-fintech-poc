from pyspark.sql.types import *
from pyspark.sql.functions import *

class Account:
    """Account model for PySpark processing"""
    
    @staticmethod
    def get_schema():
        """Get Spark SQL schema for Account"""
        return StructType([
            StructField("id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("account_type", StringType(), True),
            StructField("account_number", StringType(), True),
            StructField("routing_number", StringType(), True),
            StructField("balance", DecimalType(10, 2), True),
            StructField("currency", StringType(), True),
            StructField("status", StringType(), True),
            StructField("opening_date", TimestampType(), True),
            StructField("last_activity_date", TimestampType(), True),
            StructField("daily_limit", DecimalType(10, 2), True),
            StructField("monthly_limit", DecimalType(10, 2), True),
            StructField("risk_score", DoubleType(), True),
            StructField("compliance_status", StringType(), True),
            StructField("features", StringType(), True),  # JSON string
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ])
    
    @staticmethod
    def parse_json_fields(df):
        """Parse JSON string fields to proper structures"""
        return df.withColumn("features", 
                           from_json(col("features"), MapType(StringType(), StringType())))
    
    @staticmethod
    def calculate_account_metrics(df):
        """Calculate account metrics"""
        return df.withColumn("days_since_opening", 
                           datediff(current_timestamp(), col("opening_date"))) \
                .withColumn("days_since_last_activity", 
                           datediff(current_timestamp(), col("last_activity_date"))) \
                .withColumn("is_active", 
                           when(col("days_since_last_activity") <= 30, True).otherwise(False)) \
                .withColumn("account_tier", 
                           when(col("balance") >= 50000, "Premium")
                           .when(col("balance") >= 10000, "Gold")
                           .when(col("balance") >= 1000, "Silver")
                           .otherwise("Basic"))
