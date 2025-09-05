# Spark Fintech Analytics Jobs (PySpark)

This module contains PySpark Streaming jobs for real-time fintech analytics, designed to mirror the functionality of the Flink jobs for performance comparison.

## Architecture

The Spark streaming pipeline follows the same data flow as the Flink pipeline:
```
Python App → PostgreSQL → Kafka Topics → PySpark Streaming → Kafka Topics → ClickHouse
```

## Core Processors

1. **Customer Lifecycle Analysis** - Enriches transactions with customer data and analyzes lifecycle metrics
2. **Merchant Performance Analysis** - Analyzes merchant performance and business insights
3. **Customer Transaction Metrics** - Provides core transaction analytics and dashboard metrics
4. **Customer Fraud Detection** - Real-time fraud detection and security monitoring

## Technologies Used

- **PySpark 3.5.0** - Python API for Apache Spark
- **Spark Streaming** - Real-time data processing
- **Kafka Integration** - Data ingestion and output
- **ClickHouse** - Data warehouse for analytics
- **Pandas** - Data manipulation and analysis

## Building and Running

### Run locally
```bash
pip install -r requirements.txt
python fintech_analytics_job.py
```

### Run in Docker
The Spark jobs are automatically built and deployed when running `docker-compose up` in the parent directory.

## Performance Comparison

This PySpark implementation is designed to provide a direct comparison with the Flink implementation, using:
- Similar data models and processing logic
- Same Kafka topics for input/output
- Same ClickHouse tables for storage
- Comparable windowing and aggregation strategies

## Monitoring

- Spark Master UI: http://localhost:8080
- Spark Worker UI: http://localhost:8081
- Kafka UI: http://localhost:8080 (Kafka UI)
- Grafana Dashboard: http://localhost:3000

## File Structure

```
spark-jobs/
├── fintech_analytics_job.py    # Main PySpark streaming job
├── models/                     # Data models
│   ├── __init__.py
│   ├── transaction.py
│   ├── customer.py
│   ├── merchant.py
│   └── ...
├── processors/                 # Processing logic
│   ├── __init__.py
│   ├── customer_lifecycle.py
│   ├── merchant_performance.py
│   └── ...
├── utils/                      # Utility functions
│   ├── __init__.py
│   ├── kafka_utils.py
│   └── clickhouse_utils.py
├── requirements.txt
├── Dockerfile
└── README.md
```