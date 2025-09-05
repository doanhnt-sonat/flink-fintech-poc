#!/usr/bin/env python3
"""
Script to run Spark streaming job with proper configuration
"""

import os
import sys
import subprocess
import time

def run_spark_job():
    """Run the Spark streaming job"""
    
    # Set environment variables
    os.environ["PYSPARK_PYTHON"] = "python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
    
    # Spark configuration
    spark_conf = {
        "spark.master": "spark://spark-master:7077",
        "spark.app.name": "FintechAnalyticsJob",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.streaming.checkpointLocation": "/tmp/checkpoint",
        "spark.sql.streaming.kafka.useDeprecatedOffsetFetching": "false"
    }
    
    # Build spark-submit command
    cmd = [
        "spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "--conf", f"spark.sql.adaptive.enabled={spark_conf['spark.sql.adaptive.enabled']}",
        "--conf", f"spark.sql.adaptive.coalescePartitions.enabled={spark_conf['spark.sql.adaptive.coalescePartitions.enabled']}",
        "--conf", f"spark.serializer={spark_conf['spark.serializer']}",
        "--conf", f"spark.sql.streaming.checkpointLocation={spark_conf['spark.sql.streaming.checkpointLocation']}",
        "--conf", "spark.sql.streaming.kafka.useDeprecatedOffsetFetching=false",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.clickhouse:clickhouse-jdbc:0.4.6",
        "fintech_analytics_job.py"
    ]
    
    print("Starting Spark streaming job...")
    print(f"Command: {' '.join(cmd)}")
    
    try:
        # Run the job
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        
        # Stream output
        for line in iter(process.stdout.readline, ''):
            print(line.rstrip())
        
        process.wait()
        
    except KeyboardInterrupt:
        print("\nStopping Spark job...")
        process.terminate()
        process.wait()
    except Exception as e:
        print(f"Error running Spark job: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_spark_job()
