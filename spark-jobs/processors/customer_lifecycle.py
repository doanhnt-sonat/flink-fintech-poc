from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Dict, List

class CustomerLifecycleProcessor:
    """Customer Lifecycle Analysis Processor"""
    
    @staticmethod
    def process_customer_lifecycle(transaction_df: DataFrame, customer_df: DataFrame, session_df: DataFrame) -> DataFrame:
        """
        Process customer lifecycle analysis by enriching transactions with customer and session data
        """
        # Enrich transactions with customer data
        enriched_df = transaction_df.join(
            customer_df.select("id", "email", "tier", "registration_date", "last_login_date", "total_transactions", "total_amount"),
            transaction_df.customer_id == customer_df.id,
            "left"
        )
        
        # Add session data
        enriched_df = enriched_df.join(
            session_df.select("customer_id", "channel", "device_type", "duration_seconds", "page_views"),
            transaction_df.customer_id == session_df.customer_id,
            "left"
        )
        
        # Calculate lifecycle metrics
        lifecycle_df = enriched_df.withColumn("days_since_registration", 
                                            datediff(current_timestamp(), col("registration_date"))) \
                                 .withColumn("days_since_last_login", 
                                            datediff(current_timestamp(), col("last_login_date"))) \
                                 .withColumn("is_active_customer", 
                                            when(col("days_since_last_login") <= 30, True).otherwise(False)) \
                                 .withColumn("customer_segment", 
                                            when(col("total_amount") >= 10000, "High Value")
                                            .when(col("total_amount") >= 1000, "Medium Value")
                                            .otherwise("Low Value")) \
                                 .withColumn("transaction_frequency", 
                                            when(col("total_transactions") >= 100, "High")
                                            .when(col("total_transactions") >= 20, "Medium")
                                            .otherwise("Low"))
        
        # Aggregate by customer for lifecycle metrics
        lifecycle_metrics = lifecycle_df.groupBy("customer_id") \
            .agg(
                first("customer_email").alias("customer_email"),
                first("customer_tier").alias("customer_tier"),
                first("customer_segment").alias("customer_segment"),
                first("is_active_customer").alias("is_active_customer"),
                first("transaction_frequency").alias("transaction_frequency"),
                count("id").alias("transaction_count"),
                sum("amount").alias("total_amount"),
                avg("amount").alias("avg_transaction_amount"),
                min("created_at").alias("first_transaction_date"),
                max("created_at").alias("last_transaction_date"),
                first("channel").alias("primary_channel"),
                first("device_type").alias("primary_device"),
                avg("duration_seconds").alias("avg_session_duration"),
                avg("page_views").alias("avg_page_views")
            )
        
        return lifecycle_metrics
    
    @staticmethod
    def detect_lifecycle_events(lifecycle_df: DataFrame) -> DataFrame:
        """
        Detect significant lifecycle events
        """
        return lifecycle_df.withColumn("lifecycle_event", 
                                     when(col("transaction_count") == 1, "First Transaction")
                                     .when(col("transaction_count") >= 100, "Power User")
                                     .when(col("is_active_customer") == False, "Churned Customer")
                                     .when(col("total_amount") >= 5000, "High Value Customer")
                                     .otherwise("Regular Customer")) \
                          .withColumn("event_timestamp", current_timestamp())
