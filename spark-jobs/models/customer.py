from pyspark.sql.types import *
from pyspark.sql.functions import *

class Customer:
    """Customer model for PySpark processing"""
    
    @staticmethod
    def get_schema():
        """Get Spark SQL schema for Customer"""
        return StructType([
            StructField("id", StringType(), True),
            StructField("email", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("date_of_birth", DateType(), True),
            StructField("address", StringType(), True),  # JSON string
            StructField("tier", StringType(), True),
            StructField("status", StringType(), True),
            StructField("registration_date", TimestampType(), True),
            StructField("last_login_date", TimestampType(), True),
            StructField("total_transactions", IntegerType(), True),
            StructField("total_amount", DecimalType(10, 2), True),
            StructField("risk_score", DoubleType(), True),
            StructField("compliance_status", StringType(), True),
            StructField("preferences", StringType(), True),  # JSON string
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ])
    
    @staticmethod
    def parse_json_fields(df):
        """Parse JSON string fields to proper structures"""
        return df.withColumn("address", 
                           from_json(col("address"), MapType(StringType(), StringType()))) \
                .withColumn("preferences", 
                           from_json(col("preferences"), MapType(StringType(), StringType())))
    
    @staticmethod
    def calculate_lifecycle_metrics(df):
        """Calculate customer lifecycle metrics"""
        return df.withColumn("days_since_registration", 
                           datediff(current_timestamp(), col("registration_date"))) \
                .withColumn("days_since_last_login", 
                           datediff(current_timestamp(), col("last_login_date"))) \
                .withColumn("is_active", 
                           when(col("days_since_last_login") <= 30, True).otherwise(False)) \
                .withColumn("customer_segment", 
                           when(col("total_amount") >= 10000, "High Value")
                           .when(col("total_amount") >= 1000, "Medium Value")
                           .otherwise("Low Value"))
