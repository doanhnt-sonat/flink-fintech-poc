package flinkfintechpoc.jobs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class CustomerDebeziumJob {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    tableEnv.executeSql(
        """
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
            """
    );

// Show the original data
//    Table resultTable = tableEnv.sqlQuery("SELECT * FROM customers_cdc");
//
//    DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
//
//    resultStream.print("Customer Data: ");


    // Aggregate
    Table result = tableEnv.sqlQuery(
        "SELECT " +
            "  TUMBLE_START(created_at, INTERVAL '30' SECOND) AS window_start, " +
            "  COUNT(*) AS customers_created " +
            "FROM customers_cdc " +
            "GROUP BY TUMBLE(created_at, INTERVAL '30' SECOND)"
    );

    result.execute().print();

    env.execute("Customer Debezium Job");
  }
}
