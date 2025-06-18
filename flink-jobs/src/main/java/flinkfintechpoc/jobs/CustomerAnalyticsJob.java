package flinkfintechpoc.jobs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * Flink job that reads from the customers Kafka topic and calculates
 * the number of customers created per minute.
 */
public class CustomerAnalyticsJob {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a Table environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Define the Kafka source table for customers using Debezium JSON format
        tableEnv.createTable("customers_cdc", 
            TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                    .column("before", DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("email", DataTypes.STRING()),
                        DataTypes.FIELD("created_at", DataTypes.TIMESTAMP(3))
                    ).nullable())
                    .column("after", DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("email", DataTypes.STRING()),
                        DataTypes.FIELD("created_at", DataTypes.TIMESTAMP(3))
                    ).nullable())
                    .column("source", DataTypes.ROW(
                        DataTypes.FIELD("version", DataTypes.STRING()),
                        DataTypes.FIELD("connector", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("ts_ms", DataTypes.BIGINT()),
                        DataTypes.FIELD("snapshot", DataTypes.STRING().nullable()),
                        DataTypes.FIELD("db", DataTypes.STRING()),
                        DataTypes.FIELD("sequence", DataTypes.STRING().nullable()),
                        DataTypes.FIELD("schema", DataTypes.STRING()),
                        DataTypes.FIELD("table", DataTypes.STRING()),
                        DataTypes.FIELD("txId", DataTypes.BIGINT().nullable()),
                        DataTypes.FIELD("lsn", DataTypes.BIGINT().nullable()),
                        DataTypes.FIELD("xmin", DataTypes.BIGINT().nullable())
                    ))
                    .column("op", DataTypes.STRING())
                    .column("ts_ms", DataTypes.BIGINT())
                    .column("transaction", DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.STRING()),
                        DataTypes.FIELD("total_order", DataTypes.BIGINT()),
                        DataTypes.FIELD("data_collection_order", DataTypes.BIGINT())
                    ).nullable())
                    .build())
                .option("topic", "customers.public.customers")
                .option("properties.bootstrap.servers", "kafka:29092")
                .option("properties.group.id", "customer-analytics")
                .option("scan.startup.mode", "latest-offset")
                .option("format", "debezium-json")
                .build());

        // Create a view that extracts the customer data from the Debezium envelope
        tableEnv.executeSql(
            "CREATE VIEW customers AS " +
            "SELECT " +
            "  after.id AS id, " +
            "  after.name AS name, " +
            "  after.email AS email, " +
            "  after.created_at AS created_at " +
            "FROM customers_cdc " +
            "WHERE op = 'c' OR op = 'r' " +  // Only consider inserts (c) and reads (r)
            "AND after IS NOT NULL"
        );

        // SQL query to count customers per minute
        String sql = 
            "SELECT " +
            "  TUMBLE_START(created_at, INTERVAL '1' MINUTE) AS window_start, " +
            "  COUNT(*) AS customer_count " +
            "FROM customers " +
            "GROUP BY TUMBLE(created_at, INTERVAL '1' MINUTE)";

        // Execute the SQL query
        Table resultTable = tableEnv.sqlQuery(sql);

        // Convert the Table back to a DataStream
        DataStream<Tuple2<Long, Long>> resultStream = 
            tableEnv.toDataStream(resultTable)
                .map(row -> {
                    java.sql.Timestamp timestamp = (java.sql.Timestamp) row.getField(0);
                    Long count = (Long) row.getField(1);
                    return new Tuple2<>(timestamp.getTime(), count);
                })
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                    org.apache.flink.api.common.typeinfo.Types.LONG,
                    org.apache.flink.api.common.typeinfo.Types.LONG
                ));

        // Print the results
        resultStream.print("Customers created per minute: ");

        // Execute the job
        env.execute("Customer Analytics Job");
    }
}
