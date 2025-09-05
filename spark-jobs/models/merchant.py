from pyspark.sql.types import *
from pyspark.sql.functions import *

class Merchant:
    """Merchant model for PySpark processing"""
    
    @staticmethod
    def get_schema():
        """Get Spark SQL schema for Merchant"""
        return StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("business_type", StringType(), True),
            StructField("category", StringType(), True),
            StructField("location", StringType(), True),  # JSON string
            StructField("contact_email", StringType(), True),
            StructField("contact_phone", StringType(), True),
            StructField("website", StringType(), True),
            StructField("status", StringType(), True),
            StructField("registration_date", TimestampType(), True),
            StructField("last_transaction_date", TimestampType(), True),
            StructField("total_transactions", IntegerType(), True),
            StructField("total_volume", DecimalType(10, 2), True),
            StructField("average_transaction_amount", DecimalType(10, 2), True),
            StructField("risk_score", DoubleType(), True),
            StructField("compliance_status", StringType(), True),
            StructField("payment_methods", StringType(), True),  # JSON string
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ])
    
    @staticmethod
    def parse_json_fields(df):
        """Parse JSON string fields to proper structures"""
        return df.withColumn("location", 
                           from_json(col("location"), MapType(StringType(), StringType()))) \
                .withColumn("payment_methods", 
                           from_json(col("payment_methods"), ArrayType(StringType())))
    
    @staticmethod
    def calculate_performance_metrics(df):
        """Calculate merchant performance metrics"""
        return df.withColumn("days_since_registration", 
                           datediff(current_timestamp(), col("registration_date"))) \
                .withColumn("days_since_last_transaction", 
                           datediff(current_timestamp(), col("last_transaction_date"))) \
                .withColumn("is_active", 
                           when(col("days_since_last_transaction") <= 7, True).otherwise(False)) \
                .withColumn("performance_tier", 
                           when(col("total_volume") >= 100000, "High Volume")
                           .when(col("total_volume") >= 10000, "Medium Volume")
                           .otherwise("Low Volume"))
