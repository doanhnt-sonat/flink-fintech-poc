from pyspark.sql.types import *
from pyspark.sql.functions import *
from typing import Dict, List, Optional
import json

class Transaction:
    """Transaction model for PySpark processing"""
    
    @staticmethod
    def get_schema():
        """Get Spark SQL schema for Transaction"""
        return StructType([
            StructField("id", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("status", StringType(), True),
            StructField("from_account_id", StringType(), True),
            StructField("to_account_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DecimalType(10, 2), True),
            StructField("currency", StringType(), True),
            StructField("exchange_rate", DecimalType(10, 4), True),
            StructField("fee_amount", DecimalType(10, 2), True),
            StructField("description", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("reference_number", StringType(), True),
            StructField("authorization_code", StringType(), True),
            StructField("transaction_location", StringType(), True),  # JSON string
            StructField("device_fingerprint", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("user_agent", StringType(), True),
            StructField("risk_score", DoubleType(), True),
            StructField("risk_level", StringType(), True),
            StructField("compliance_flags", StringType(), True),  # JSON string
            StructField("processing_time_ms", IntegerType(), True),
            StructField("network", StringType(), True),
            StructField("card_last_four", StringType(), True),
            StructField("tags", StringType(), True),  # JSON string
            StructField("metadata", StringType(), True),  # JSON string
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ])
    
    @staticmethod
    def parse_json_fields(df):
        """Parse JSON string fields to proper structures"""
        return df.withColumn("transaction_location", 
                           from_json(col("transaction_location"), MapType(StringType(), StringType()))) \
                .withColumn("compliance_flags", 
                           from_json(col("compliance_flags"), ArrayType(StringType()))) \
                .withColumn("tags", 
                           from_json(col("tags"), ArrayType(StringType()))) \
                .withColumn("metadata", 
                           from_json(col("metadata"), MapType(StringType(), StringType())))
    
    @staticmethod
    def enrich_with_customer_data(transaction_df, customer_df):
        """Enrich transactions with customer data"""
        return transaction_df.join(
            customer_df.select("id", "email", "tier", "registration_date", "last_login_date"),
            transaction_df.customer_id == customer_df.id,
            "left"
        ).withColumnRenamed("email", "customer_email") \
         .withColumnRenamed("tier", "customer_tier") \
         .withColumnRenamed("registration_date", "customer_registration_date") \
         .withColumnRenamed("last_login_date", "customer_last_login_date")
    
    @staticmethod
    def enrich_with_merchant_data(transaction_df, merchant_df):
        """Enrich transactions with merchant data"""
        return transaction_df.join(
            merchant_df.select("id", "name", "business_type", "category", "location"),
            transaction_df.merchant_id == merchant_df.id,
            "left"
        ).withColumnRenamed("name", "merchant_name") \
         .withColumnRenamed("business_type", "merchant_business_type") \
         .withColumnRenamed("category", "merchant_category") \
         .withColumnRenamed("location", "merchant_location")
