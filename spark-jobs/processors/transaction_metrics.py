from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import *
from typing import Dict, List

class TransactionMetricsProcessor:
    """Transaction Metrics Processor"""
    
    @staticmethod
    def process_transaction_metrics(transaction_df: DataFrame) -> DataFrame:
        """
        Process transaction metrics for dashboard and analytics
        """
        # Calculate transaction metrics by customer
        customer_metrics = transaction_df.groupBy("customer_id") \
            .agg(
                count("id").alias("transaction_count"),
                sum("amount").alias("total_amount"),
                avg("amount").alias("avg_transaction_amount"),
                min("amount").alias("min_transaction_amount"),
                max("amount").alias("max_transaction_amount"),
                countDistinct("merchant_id").alias("unique_merchants"),
                countDistinct("transaction_type").alias("transaction_types"),
                sum(when(col("status") == "completed", 1).otherwise(0)).alias("completed_transactions"),
                sum(when(col("status") == "failed", 1).otherwise(0)).alias("failed_transactions"),
                sum(when(col("status") == "pending", 1).otherwise(0)).alias("pending_transactions"),
                avg("risk_score").alias("avg_risk_score"),
                min("created_at").alias("first_transaction_date"),
                max("created_at").alias("last_transaction_date"),
                sum("fee_amount").alias("total_fees")
            )
        
        # Calculate additional metrics
        customer_metrics = customer_metrics.withColumn("success_rate", 
                                                     col("completed_transactions") / col("transaction_count")) \
                                          .withColumn("days_active", 
                                                     datediff(col("last_transaction_date"), col("first_transaction_date"))) \
                                          .withColumn("transactions_per_day", 
                                                     col("transaction_count") / greatest(col("days_active"), 1)) \
                                          .withColumn("avg_merchant_value", 
                                                     col("total_amount") / col("unique_merchants")) \
                                          .withColumn("customer_tier", 
                                                     when(col("total_amount") >= 10000, "High Value")
                                                     .when(col("total_amount") >= 1000, "Medium Value")
                                                     .otherwise("Low Value"))
        
        return customer_metrics
    
    @staticmethod
    def process_hourly_metrics(transaction_df: DataFrame) -> DataFrame:
        """
        Process hourly transaction metrics
        """
        return transaction_df.withColumn("hour", hour(col("created_at"))) \
                            .groupBy("hour") \
                            .agg(
                                count("id").alias("transaction_count"),
                                sum("amount").alias("total_volume"),
                                avg("amount").alias("avg_transaction_amount"),
                                countDistinct("customer_id").alias("unique_customers"),
                                countDistinct("merchant_id").alias("unique_merchants")
                            ) \
                            .orderBy("hour")
    
    @staticmethod
    def process_daily_metrics(transaction_df: DataFrame) -> DataFrame:
        """
        Process daily transaction metrics
        """
        return transaction_df.withColumn("date", to_date(col("created_at"))) \
                            .groupBy("date") \
                            .agg(
                                count("id").alias("transaction_count"),
                                sum("amount").alias("total_volume"),
                                avg("amount").alias("avg_transaction_amount"),
                                countDistinct("customer_id").alias("unique_customers"),
                                countDistinct("merchant_id").alias("unique_merchants"),
                                sum("fee_amount").alias("total_fees")
                            ) \
                            .orderBy("date")
