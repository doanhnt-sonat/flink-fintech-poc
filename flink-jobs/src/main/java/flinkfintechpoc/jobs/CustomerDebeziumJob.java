package flinkfintechpoc.jobs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * Flink job that reads data from Kafka topic (customers.public.customers) with Debezium JSON format,
 * builds a customers table, and performs an aggregation to count the number of users created in 30-second windows.
 */
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
              created_at STRING,
              created_ts AS TO_TIMESTAMP(SUBSTR(created_at, 1, 23) || 'Z'),
              updated_at STRING,
              PRIMARY KEY (id) NOT ENFORCED
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'customers.public.customers',
              'properties.bootstrap.servers' = 'kafka:29092',
              'properties.group.id' = 'testGroup1',
              'scan.startup.mode' = 'earliest-offset',
              'format' = 'debezium-json',
              'debezium-json.ignore-parse-errors' = 'true',
              'debezium-json.schema-include' = 'false'
            )
            """
        );

      tableEnv.executeSql(
          """
          CREATE TABLE customers_sink (
            id INT,
            name STRING,
            email STRING,
            created_at STRING,
            created_ts TIMESTAMP(3),
            updated_at STRING,
            PRIMARY KEY (id) NOT ENFORCED
          ) WITH (
            'connector' = 'print'
          )
          """
      );

      // Execute the SQL statement and get the result
      Table resultTable = tableEnv.sqlQuery("SELECT * FROM customers_cdc");

      // Convert the Table to a Changelog DataStream to handle CDC events
      DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

      // Print the results
      resultStream.print("Customer Data: ");

      // Also execute the insert statement
      tableEnv.executeSql(
          "INSERT INTO customers_sink SELECT * FROM customers_cdc"
      );

        // Create a view that extracts the relevant fields from the Debezium format
//        tableEnv.executeSql(
//            "CREATE VIEW customer_events AS " +
//            "SELECT " +
//            "  after.id AS customer_id, " +
//            "  after.name AS customer_name, " +
//            "  after.email AS customer_email, " +
//            "  after.created_at AS created_at, " +
//            "  proc_time " +
//            "FROM customers " +
//            "WHERE op = 'c'"  // Only count 'create' operations
//        );

        // SQL query to count customers created in 30-second windows
//        String sql =
//            "SELECT " +
//            "  TUMBLE_START(proc_time, INTERVAL '30' SECOND) AS window_start, " +
//            "  COUNT(*) AS customer_count " +
//            "FROM customer_events " +
//            "GROUP BY TUMBLE(proc_time, INTERVAL '30' SECOND)";

//        // Execute the SQL query
//        Table resultTable = tableEnv.sqlQuery(sql);
//
//        // Convert the Table back to a DataStream
//        DataStream<Tuple2<String, Long>> resultStream =
//            tableEnv.toDataStream(resultTable)
//                .map(row -> {
//                    String windowStart = row.getField(0).toString();
//                    Long count = (Long) row.getField(1);
//                    return new Tuple2<>(windowStart, count);
//                })
//                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
//                    org.apache.flink.api.common.typeinfo.Types.STRING,
//                    org.apache.flink.api.common.typeinfo.Types.LONG
//                ));
//
//        // Print the results
//        resultStream.print("Customers created per 30-second window: ");

        // Execute the job
        env.execute("Customer Debezium Job");
    }
}
