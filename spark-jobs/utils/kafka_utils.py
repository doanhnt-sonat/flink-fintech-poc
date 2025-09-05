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
    def create_kafka_sink(df, topic: str, kafka_servers: str = "kafka:29092", key_column: str | None = "customer_id"):
        """
        Create a Kafka sink for streaming data
        """
        key_col = col(key_column).cast("string").alias("key") if key_column else lit(None).cast("string").alias("key")
        return df.select(
            key_col,
            to_json(struct("*")).alias("value")
        ).writeStream \
         .format("kafka") \
         .option("kafka.bootstrap.servers", kafka_servers) \
         .option("topic", topic) \
         .option("checkpointLocation", f"/tmp/checkpoint/{topic}") \
         .outputMode("append")
    
    @staticmethod
    def create_kafka_sink_batch(df, topic: str, kafka_servers: str = "kafka:29092", key_column: str | None = "customer_id"):
        """
        Create a Kafka sink for batch data
        """
        key_col = col(key_column).cast("string").alias("key") if key_column else lit(None).cast("string").alias("key")
        return df.select(
            key_col,
            to_json(struct("*")).alias("value")
        ).write \
         .format("kafka") \
         .option("kafka.bootstrap.servers", kafka_servers) \
         .option("topic", topic) \
         .save()

    @staticmethod
    def read_kafka_batch_snapshot(spark: SparkSession, topic: str, schema: StructType, key_column: str | None, kafka_servers: str = "kafka:29092"):
        """
        Read a Kafka topic as a batch snapshot and return the latest record per key.
        - Requires a key to deduplicate. If key_column is None, uses Kafka message key.
        - Assumes value is JSON matching the provided schema.
        """
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()

        parsed = df.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("value"),
            col("timestamp").alias("kafka_timestamp")
        ).select(
            col("kafka_key"),
            from_json(col("value"), schema).alias("data"),
            col("kafka_timestamp")
        ).select(
            col("kafka_key"), col("kafka_timestamp"), col("data.*")
        )

        # Choose the dedup key
        key_expr = col(key_column) if key_column else col("kafka_key")

        # Latest-by-key using row_number over timestamp desc
        from pyspark.sql.window import Window
        w = Window.partitionBy(key_expr).orderBy(col("kafka_timestamp").desc())
        return parsed.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")
