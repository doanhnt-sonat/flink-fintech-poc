from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Dict, List

class FraudDetectionProcessor:
    """Fraud Detection Processor"""
    
    @staticmethod
    def detect_fraud_patterns(transaction_df: DataFrame, account_static_df: DataFrame) -> DataFrame:
        """
        Detect fraud patterns in transactions
        """
        # Enrich transactions with account data
        enriched_df = transaction_df.join(
            broadcast(account_static_df.select("id", "customer_id", "account_type", "balance", "daily_limit", "monthly_limit", "risk_score")),
            transaction_df.from_account_id == account_static_df.id,
            "left"
        )
        
        # Calculate fraud indicators
        fraud_df = enriched_df.withColumn("amount_to_balance_ratio", 
                                        col("amount") / greatest(col("balance"), 1)) \
                             .withColumn("amount_to_daily_limit_ratio", 
                                        col("amount") / greatest(col("daily_limit"), 1)) \
                             .withColumn("is_high_amount", 
                                        when(col("amount") >= 1000, True).otherwise(False)) \
                             .withColumn("is_high_risk", 
                                        when(col("risk_score") >= 0.7, True).otherwise(False)) \
                             .withColumn("is_suspicious_amount", 
                                        when(col("amount_to_balance_ratio") >= 0.5, True).otherwise(False)) \
                             .withColumn("is_over_daily_limit", 
                                        when(col("amount") > col("daily_limit"), True).otherwise(False))
        
        # Calculate fraud score
        fraud_df = fraud_df.withColumn("fraud_score", 
                                     (col("risk_score") * 0.4) + 
                                     (when(col("is_high_amount"), 0.3).otherwise(0)) +
                                     (when(col("is_suspicious_amount"), 0.2).otherwise(0)) +
                                     (when(col("is_over_daily_limit"), 0.1).otherwise(0))) \
                          .withColumn("is_fraudulent", 
                                     when(col("fraud_score") >= 0.6, True).otherwise(False))
        
        return fraud_df
    
    @staticmethod
    def detect_velocity_anomalies(transaction_df: DataFrame) -> DataFrame:
        """
        Detect velocity-based fraud patterns
        """
        # Calculate transaction velocity by customer
        velocity_df = transaction_df.withColumn("hour", hour(col("created_at"))) \
                                  .groupBy("customer_id", "hour") \
                                  .agg(
                                      count("id").alias("hourly_transactions"),
                                      sum("amount").alias("hourly_amount")
                                  )
        
        # Calculate velocity metrics
        velocity_metrics = velocity_df.groupBy("customer_id") \
            .agg(
                max("hourly_transactions").alias("max_hourly_transactions"),
                avg("hourly_transactions").alias("avg_hourly_transactions"),
                max("hourly_amount").alias("max_hourly_amount"),
                avg("hourly_amount").alias("avg_hourly_amount")
            )
        
        # Detect velocity anomalies
        velocity_anomalies = velocity_metrics.withColumn("is_velocity_anomaly", 
                                                       when(col("max_hourly_transactions") >= 10, True)
                                                       .when(col("max_hourly_amount") >= 5000, True)
                                                       .otherwise(False))
        
        return velocity_anomalies
    
    @staticmethod
    def detect_geographic_anomalies(transaction_df: DataFrame) -> DataFrame:
        """
        Detect geographic-based fraud patterns
        """
        # Parse location data and detect anomalies
        geo_df = transaction_df.filter(col("transaction_location").isNotNull()) \
                              .withColumn("location_country", 
                                        col("transaction_location")["country"]) \
                              .withColumn("location_city", 
                                        col("transaction_location")["city"])
        
        # Calculate geographic patterns
        geo_patterns = geo_df.groupBy("customer_id") \
            .agg(
                countDistinct("location_country").alias("unique_countries"),
                countDistinct("location_city").alias("unique_cities"),
                count("id").alias("total_transactions")
            )
        
        # Detect geographic anomalies
        geo_anomalies = geo_patterns.withColumn("is_geo_anomaly", 
                                              when(col("unique_countries") >= 3, True)
                                              .when(col("unique_cities") >= 10, True)
                                              .otherwise(False))
        
        return geo_anomalies
