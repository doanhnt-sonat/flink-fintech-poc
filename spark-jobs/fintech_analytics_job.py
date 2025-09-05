#!/usr/bin/env python3
"""
PySpark Streaming Job for Real-time Fintech Analytics

This job processes streaming data from Kafka topics and provides:
1. Customer Lifecycle Analysis
2. Merchant Performance Analysis  
3. Customer Transaction Metrics
4. Customer Fraud Detection

Pipeline: Python App → PostgreSQL → Kafka Topics → PySpark Streaming → Kafka Topics → ClickHouse
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
import logging

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from models.transaction import Transaction
from models.customer import Customer
from models.merchant import Merchant
from models.customer_session import CustomerSession
from models.account import Account
from processors.customer_lifecycle import CustomerLifecycleProcessor
from processors.merchant_performance import MerchantPerformanceProcessor
from processors.transaction_metrics import TransactionMetricsProcessor
from processors.fraud_detection import FraudDetectionProcessor
from utils.kafka_utils import KafkaUtils

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FintechAnalyticsJob:
    """Main PySpark Streaming Job for Fintech Analytics"""
    
    def __init__(self):
        self.spark = None
        self.kafka_servers = "kafka:29092"
        # No direct ClickHouse writes here; Spark outputs to Kafka only
        
    def create_spark_session(self):
        """Create Spark session with required configurations"""
        self.spark = SparkSession.builder \
            .appName("FintechAnalyticsJob") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .getOrCreate()
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        
    def create_kafka_sources(self):
        """Create Kafka source DataFrames for all topics"""
        logger.info("Creating Kafka sources...")
        
        # Kafka topics (Debezium format: topic_prefix.schema.table)
        topics = {
            "transactions": "fintech.public.transactions",
            "customers": "fintech.public.customers", 
            "merchants": "fintech.public.merchants",
            "customer_sessions": "fintech.public.customer_sessions",
            "accounts": "fintech.public.accounts"
        }
        
        sources = {}
        for name, topic in topics.items():
            logger.info(f"Creating source for topic: {topic}")
            sources[name] = KafkaUtils.create_kafka_source(
                self.spark, topic, self.kafka_servers
            )
        
        return sources
    
    def parse_kafka_data(self, sources):
        """Parse Kafka messages and convert to structured DataFrames"""
        logger.info("Parsing Kafka data...")
        
        # Parse each source
        parsed_data = {}
        
        # Parse transactions
        transaction_schema = Transaction.get_schema()
        parsed_data["transactions"] = KafkaUtils.parse_kafka_message(
            sources["transactions"], transaction_schema
        )
        
        # Parse customers
        customer_schema = Customer.get_schema()
        parsed_data["customers"] = KafkaUtils.parse_kafka_message(
            sources["customers"], customer_schema
        )
        
        # Parse merchants
        merchant_schema = Merchant.get_schema()
        parsed_data["merchants"] = KafkaUtils.parse_kafka_message(
            sources["merchants"], merchant_schema
        )
        
        # Parse customer sessions
        session_schema = CustomerSession.get_schema()
        parsed_data["customer_sessions"] = KafkaUtils.parse_kafka_message(
            sources["customer_sessions"], session_schema
        )
        
        # Parse accounts
        account_schema = Account.get_schema()
        parsed_data["accounts"] = KafkaUtils.parse_kafka_message(
            sources["accounts"], account_schema
        )
        
        return parsed_data
    
    def process_customer_lifecycle(self, parsed_data):
        """Process customer lifecycle analysis"""
        logger.info("Processing customer lifecycle analysis...")
        
        # Get data
        transaction_df = parsed_data["transactions"]
        customer_df = parsed_data["customers"]
        session_df = parsed_data["customer_sessions"]
        
        # Process lifecycle metrics
        lifecycle_metrics = CustomerLifecycleProcessor.process_customer_lifecycle(
            transaction_df, customer_df, session_df
        )
        
        # Detect lifecycle events
        lifecycle_events = CustomerLifecycleProcessor.detect_lifecycle_events(lifecycle_metrics)
        
        return lifecycle_events
    
    def process_merchant_performance(self, parsed_data):
        """Process merchant performance analysis"""
        logger.info("Processing merchant performance analysis...")
        
        # Get data
        transaction_df = parsed_data["transactions"]
        merchant_df = parsed_data["merchants"]
        
        # Process merchant performance
        merchant_metrics = MerchantPerformanceProcessor.process_merchant_performance(
            transaction_df, merchant_df
        )
        
        # Detect anomalies
        merchant_anomalies = MerchantPerformanceProcessor.detect_merchant_anomalies(merchant_metrics)
        
        return merchant_anomalies
    
    def process_transaction_metrics(self, parsed_data):
        """Process transaction metrics"""
        logger.info("Processing transaction metrics...")
        
        # Get data
        transaction_df = parsed_data["transactions"]
        
        # Process transaction metrics
        transaction_metrics = TransactionMetricsProcessor.process_transaction_metrics(transaction_df)
        
        return transaction_metrics
    
    def process_fraud_detection(self, parsed_data):
        """Process fraud detection"""
        logger.info("Processing fraud detection...")
        
        # Get data
        transaction_df = parsed_data["transactions"]
        account_df = parsed_data["accounts"]
        
        # Detect fraud patterns
        fraud_results = FraudDetectionProcessor.detect_fraud_patterns(transaction_df, account_df)
        
        # Detect velocity anomalies
        velocity_anomalies = FraudDetectionProcessor.detect_velocity_anomalies(transaction_df)
        
        # Detect geographic anomalies
        geo_anomalies = FraudDetectionProcessor.detect_geographic_anomalies(transaction_df)
        
        # Combine fraud results
        fraud_combined = fraud_results.join(
            velocity_anomalies, "customer_id", "left"
        ).join(
            geo_anomalies, "customer_id", "left"
        ).withColumn("fraud_indicators", 
                    concat_ws(",", 
                             when(col("is_high_risk"), "High Risk").otherwise(""),
                             when(col("is_velocity_anomaly"), "Velocity Anomaly").otherwise(""),
                             when(col("is_geo_anomaly"), "Geo Anomaly").otherwise("")
                    )) \
         .withColumn("risk_factors", 
                    concat_ws(",", 
                             when(col("is_suspicious_amount"), "Suspicious Amount").otherwise(""),
                             when(col("is_over_daily_limit"), "Over Daily Limit").otherwise("")
                    ))
        
        return fraud_combined
    
    def write_to_kafka(self, df, topic_name):
        """Write DataFrame to Kafka topic"""
        logger.info(f"Writing to Kafka topic: {topic_name}")
        
        return KafkaUtils.create_kafka_sink(df, topic_name, self.kafka_servers)
    
    # Note: No direct ClickHouse sink; ClickHouse ingests from Kafka via Kafka Engine / MVs
    
    def run_streaming_job(self):
        """Run the main streaming job"""
        try:
            logger.info("Starting Fintech Analytics Streaming Job...")
            
            # Create Spark session
            self.create_spark_session()
            
            # ClickHouse tables are provisioned via SQL init; no direct creation here
            
            # Create Kafka sources
            sources = self.create_kafka_sources()
            
            # Parse Kafka data
            parsed_data = self.parse_kafka_data(sources)
            
            # Process customer lifecycle
            lifecycle_df = self.process_customer_lifecycle(parsed_data)
            
            # Process merchant performance
            merchant_df = self.process_merchant_performance(parsed_data)
            
            # Process transaction metrics
            transaction_metrics_df = self.process_transaction_metrics(parsed_data)
            
            # Process fraud detection
            fraud_df = self.process_fraud_detection(parsed_data)
            
            # Write results to Kafka
            logger.info("Writing results to Kafka topics...")
            
            # Customer lifecycle metrics
            lifecycle_query = self.write_to_kafka(
                lifecycle_df, "fintech.customer_lifecycle"
            )
            
            # Merchant performance metrics
            merchant_query = self.write_to_kafka(
                merchant_df, "fintech.merchant_performance"
            )
            
            # Transaction metrics
            transaction_query = self.write_to_kafka(
                transaction_metrics_df, "fintech.customer_transaction_metrics"
            )
            
            # Fraud detection results
            fraud_query = self.write_to_kafka(
                fraud_df, "fintech.customer_fraud_alerts"
            )
            
            # Start all streaming queries
            logger.info("Starting streaming queries...")
            
            lifecycle_query.start()
            merchant_query.start()
            transaction_query.start()
            fraud_query.start()
            
            # Wait for termination
            logger.info("All streaming queries started. Waiting for termination...")
            self.spark.streams.awaitAnyTermination()
            
        except Exception as e:
            logger.error(f"Error in streaming job: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")

def main():
    """Main entry point"""
    job = FintechAnalyticsJob()
    job.run_streaming_job()

if __name__ == "__main__":
    main()
