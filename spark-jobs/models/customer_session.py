from pyspark.sql.types import *
from pyspark.sql.functions import *

class CustomerSession:
    """Customer Session model for PySpark processing"""
    
    @staticmethod
    def get_schema():
        """Get Spark SQL schema for CustomerSession"""
        return StructType([
            StructField("id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("browser", StringType(), True),
            StructField("os", StringType(), True),
            StructField("location", StringType(), True),  # JSON string
            StructField("ip_address", StringType(), True),
            StructField("user_agent", StringType(), True),
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("duration_seconds", IntegerType(), True),
            StructField("page_views", IntegerType(), True),
            StructField("actions", StringType(), True),  # JSON string
            StructField("is_mobile", BooleanType(), True),
            StructField("is_secure", BooleanType(), True),
            StructField("referrer", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ])
    
    @staticmethod
    def parse_json_fields(df):
        """Parse JSON string fields to proper structures"""
        return df.withColumn("location", 
                           from_json(col("location"), MapType(StringType(), StringType()))) \
                .withColumn("actions", 
                           from_json(col("actions"), ArrayType(StringType())))
    
    @staticmethod
    def calculate_session_metrics(df):
        """Calculate session metrics"""
        return df.withColumn("session_duration_minutes", 
                           col("duration_seconds") / 60.0) \
                .withColumn("is_long_session", 
                           when(col("duration_seconds") >= 1800, True).otherwise(False)) \
                .withColumn("is_high_engagement", 
                           when(col("page_views") >= 10, True).otherwise(False))
