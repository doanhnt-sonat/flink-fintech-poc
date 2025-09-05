from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Dict, List

class MerchantPerformanceProcessor:
    """Merchant Performance Analysis Processor"""
    
    @staticmethod
    def process_merchant_performance(transaction_df: DataFrame, merchant_static_df: DataFrame) -> DataFrame:
        """
        Process merchant performance analysis
        """
        # Enrich transactions with merchant data
        enriched_df = transaction_df.join(
            broadcast(merchant_static_df.select("id", "name", "business_type", "category", "location", "status")),
            transaction_df.merchant_id == merchant_static_df.id,
            "left"
        )
        
        # Calculate merchant performance metrics
        merchant_metrics = enriched_df.groupBy("merchant_id") \
            .agg(
                first("merchant_name").alias("merchant_name"),
                first("business_type").alias("business_type"),
                first("category").alias("category"),
                first("status").alias("merchant_status"),
                count("id").alias("transaction_count"),
                sum("amount").alias("total_volume"),
                avg("amount").alias("avg_transaction_amount"),
                min("amount").alias("min_transaction_amount"),
                max("amount").alias("max_transaction_amount"),
                countDistinct("customer_id").alias("unique_customers"),
                min("created_at").alias("first_transaction_date"),
                max("created_at").alias("last_transaction_date"),
                sum(when(col("status") == "completed", 1).otherwise(0)).alias("completed_transactions"),
                sum(when(col("status") == "failed", 1).otherwise(0)).alias("failed_transactions"),
                avg("risk_score").alias("avg_risk_score")
            )
        
        # Calculate additional metrics
        merchant_metrics = merchant_metrics.withColumn("success_rate", 
                                                     col("completed_transactions") / col("transaction_count")) \
                                          .withColumn("days_active", 
                                                     datediff(col("last_transaction_date"), col("first_transaction_date"))) \
                                          .withColumn("transactions_per_day", 
                                                     col("transaction_count") / greatest(col("days_active"), 1)) \
                                          .withColumn("avg_customer_value", 
                                                     col("total_volume") / col("unique_customers")) \
                                          .withColumn("performance_tier", 
                                                     when(col("total_volume") >= 100000, "High Volume")
                                                     .when(col("total_volume") >= 10000, "Medium Volume")
                                                     .otherwise("Low Volume"))
        
        return merchant_metrics
    
    @staticmethod
    def detect_merchant_anomalies(merchant_df: DataFrame) -> DataFrame:
        """
        Detect merchant performance anomalies
        """
        return merchant_df.withColumn("is_anomaly", 
                                    when(col("success_rate") < 0.8, True)
                                    .when(col("avg_risk_score") > 0.7, True)
                                    .when(col("failed_transactions") > col("completed_transactions"), True)
                                    .otherwise(False)) \
                         .withColumn("anomaly_type", 
                                   when(col("success_rate") < 0.8, "Low Success Rate")
                                   .when(col("avg_risk_score") > 0.7, "High Risk")
                                   .when(col("failed_transactions") > col("completed_transactions"), "High Failure Rate")
                                   .otherwise("Normal"))
