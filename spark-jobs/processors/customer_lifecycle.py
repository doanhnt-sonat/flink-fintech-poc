from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Dict, List
from .session_processor import SessionProcessor

class CustomerLifecycleProcessor:
    """Customer Lifecycle Analysis Processor"""
    
    @staticmethod
    def process_customer_lifecycle(transaction_df: DataFrame, customer_static_df: DataFrame, session_df: DataFrame) -> DataFrame:
        """
        Process customer lifecycle analysis by enriching transactions with customer and session data
        Uses 5-minute processing-time window for sessions to avoid unbounded state
        """
        # Process sessions with 5-minute window to get latest session data per customer
        windowed_sessions = SessionProcessor.process_sessions_with_window(session_df)
        
        # Enrich transactions with customer data
        enriched_df = transaction_df.join(
            broadcast(customer_static_df.select("id", "email", "tier", "registration_date", "last_login_date", "total_transactions", "total_amount")),
            transaction_df.customer_id == customer_static_df.id,
            "left"
        )
        
        # Add windowed session data (latest session per customer within 5-minute window)
        enriched_df = enriched_df.join(
            windowed_sessions.select("customer_id", "channel", "device_type", "duration_seconds", "page_views", 
                                   "session_count_in_window", "avg_duration_in_window", "avg_page_views_in_window",
                                   "latest_session_time", "window_start", "window_end"),
            transaction_df.customer_id == windowed_sessions.customer_id,
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
                # Use windowed session metrics instead of raw session data
                first("avg_duration_in_window").alias("avg_session_duration"),
                first("avg_page_views_in_window").alias("avg_page_views"),
                first("session_count_in_window").alias("sessions_in_window"),
                first("latest_session_time").alias("latest_session_time"),
                first("window_start").alias("session_window_start"),
                first("window_end").alias("session_window_end")
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
