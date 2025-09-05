from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from typing import Dict, List

class SessionProcessor:
    """Customer Session Processing with 5-minute processing-time window"""
    
    @staticmethod
    def process_sessions_with_window(session_df: DataFrame) -> DataFrame:
        """
        Process customer sessions with 5-minute processing-time window
        to get the latest session data per customer within each window
        """
        # Add processing time column
        df_pt = session_df.withColumn("processing_time", current_timestamp())
        
        # Group by 5-minute processing-time window and customer_id
        # Get the latest session data within each window
        windowed_sessions = df_pt.groupBy(
            window(col("processing_time"), "5 minutes"),
            col("customer_id")
        ).agg(
            # Get the latest session data (most recent created_at within window)
            last("id", ignorenulls=True).alias("session_id"),
            last("channel", ignorenulls=True).alias("channel"),
            last("device_type", ignorenulls=True).alias("device_type"),
            last("duration_seconds", ignorenulls=True).alias("duration_seconds"),
            last("page_views", ignorenulls=True).alias("page_views"),
            last("ip_address", ignorenulls=True).alias("ip_address"),
            last("user_agent", ignorenulls=True).alias("user_agent"),
            last("created_at", ignorenulls=True).alias("session_created_at"),
            last("updated_at", ignorenulls=True).alias("session_updated_at"),
            # Calculate aggregated metrics within the window
            count("id").alias("session_count_in_window"),
            avg("duration_seconds").alias("avg_duration_in_window"),
            avg("page_views").alias("avg_page_views_in_window"),
            max("created_at").alias("latest_session_time"),
            min("created_at").alias("earliest_session_time")
        )
        
        # Flatten window to explicit start/end columns
        processed_sessions = windowed_sessions.select(
            col("customer_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "session_id",
            "channel",
            "device_type", 
            "duration_seconds",
            "page_views",
            "ip_address",
            "user_agent",
            "session_created_at",
            "session_updated_at",
            "session_count_in_window",
            "avg_duration_in_window",
            "avg_page_views_in_window",
            "latest_session_time",
            "earliest_session_time"
        )
        
        return processed_sessions
    
    @staticmethod
    def get_latest_sessions_per_customer(session_df: DataFrame) -> DataFrame:
        """
        Alternative approach: Get the latest session per customer without windowing
        This can be used if we want to avoid windowing complexity
        """
        # Get the latest session per customer based on created_at
        latest_sessions = session_df.withColumn("row_num", 
            row_number().over(
                Window.partitionBy("customer_id")
                .orderBy(desc("created_at"))
            )
        ).filter(col("row_num") == 1).drop("row_num")
        
        return latest_sessions
