#!/usr/bin/env python3
"""
Customer Debezium Job in Python
Equivalent to the Java CustomerDebeziumJob
Reads CDC data from Kafka using Debezium format
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def main():
    """Main function to execute the Customer Debezium job"""
    
    # Set up the streaming execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Create a Table environment
    table_env = StreamTableEnvironment.create(env)
    
    # Create the CDC source table for customers
    table_env.execute_sql("""
        CREATE TABLE customers_cdc (
            id INT,
            name STRING,
            email STRING,
            created_at TIMESTAMP_LTZ(3),
            updated_at TIMESTAMP_LTZ,
            WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'customers.public.customers',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'testGroup1',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'debezium-json',
            'debezium-json.timestamp-format.standard' = 'ISO-8601',
            'debezium-json.ignore-parse-errors' = 'true',
            'debezium-json.schema-include' = 'false'
        )
    """)
    
    # Uncomment to show the original data
    # result_table = table_env.sql_query("SELECT * FROM customers_cdc")
    # result_stream = table_env.to_changelog_stream(result_table)
    # result_stream.print("Customer Data: ")
    
    # Aggregate query - count customers created per 30 second window
    aggregation_query = """
        SELECT 
            TUMBLE_START(created_at, INTERVAL '30' SECOND) AS window_start,
            COUNT(*) AS customers_created 
        FROM customers_cdc 
        GROUP BY TUMBLE(created_at, INTERVAL '30' SECOND)
    """
    
    # Execute the aggregation query and print results
    result = table_env.sql_query(aggregation_query)
    
    print("Starting Customer Debezium Job...")
    print("Customers created per 30 seconds:")
    result.execute().print()
    
    # Execute the job
    env.execute("Python Customer Debezium Job")


if __name__ == "__main__":
    main()
