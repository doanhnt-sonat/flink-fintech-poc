from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

class KafkaUtils:
    """Utility class for Kafka operations"""
    
    @staticmethod
    def create_kafka_source(spark: SparkSession, topic: str, kafka_servers: str = "kafka:29092"):
        """
        Create a Kafka source DataFrame
        """
        return spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load()
    
    @staticmethod
    def parse_kafka_message(df, schema):
        """
        Parse Kafka message and convert to DataFrame with schema
        """
        return df.select(
            col("key").cast("string"),
            col("value").cast("string"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp")
        ).select(
            col("key"),
            from_json(col("value"), schema).alias("data"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp")
        ).select(
            col("key"),
            col("data.*"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp")
        )
    
    @staticmethod
    def create_kafka_sink(df, topic: str, kafka_servers: str = "kafka:29092"):
        """
        Create a Kafka sink for streaming data
        """
        return df.select(
            col("customer_id").cast("string").alias("key"),
            to_json(struct("*")).alias("value")
        ).writeStream \
         .format("kafka") \
         .option("kafka.bootstrap.servers", kafka_servers) \
         .option("topic", topic) \
         .option("checkpointLocation", f"/tmp/checkpoint/{topic}") \
         .outputMode("append")
    
    @staticmethod
    def create_kafka_sink_batch(df, topic: str, kafka_servers: str = "kafka:29092"):
        """
        Create a Kafka sink for batch data
        """
        return df.select(
            col("customer_id").cast("string").alias("key"),
            to_json(struct("*")).alias("value")
        ).write \
         .format("kafka") \
         .option("kafka.bootstrap.servers", kafka_servers) \
         .option("topic", topic) \
         .save()
