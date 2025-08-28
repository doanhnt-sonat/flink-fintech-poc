#!/usr/bin/env python3
"""
Customer Analytics Job in Python
Equivalent to the Java CustomerAnalyticsJob
Reads from mock data and calculates customers created per minute
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, Schema, TableDescriptor
from datetime import datetime, timezone
import time


def main():
    """Main function to execute the Customer Analytics job"""
    
    # Set up the streaming execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Create a Table environment
    table_env = StreamTableEnvironment.create(env)
    
    # Define a mock source table for customers using the datagen connector
    table_env.create_table(
        "mock_customers",
        TableDescriptor.for_connector("datagen")
        .schema(
            Schema.new_builder()
            .column("id", DataTypes.STRING())
            .column("name", DataTypes.STRING()) 
            .column("email", DataTypes.STRING())
            .column("created_at", DataTypes.TIMESTAMP(3))
            .build()
        )
        .option("rows-per-second", "5")
        .build()
    )
    
    # Create a view with a processing time attribute
    table_env.execute_sql("""
        CREATE VIEW customers_with_proctime AS 
        SELECT *, PROCTIME() AS proc_time 
        FROM mock_customers
    """)
    
    # SQL query to count customers per minute using processing time
    sql_query = """
        SELECT 
            TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS window_start,
            COUNT(*) AS customer_count 
        FROM customers_with_proctime 
        GROUP BY TUMBLE(proc_time, INTERVAL '1' MINUTE)
    """
    
    # Execute the SQL query and print results
    result_table = table_env.sql_query(sql_query)
    
    # Print the results directly from the table
    print("Starting Customer Analytics Job...")
    print("Customers created per minute:")
    result_table.execute().print()
    
    # Execute the job
    env.execute("Python Customer Analytics Job")


if __name__ == "__main__":
    main()
