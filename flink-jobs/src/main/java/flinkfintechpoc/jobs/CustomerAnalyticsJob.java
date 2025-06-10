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

        // Define the Kafka source table for customers
        tableEnv.createTable("customers", 
            TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                    .column("id", DataTypes.STRING())
                    .column("name", DataTypes.STRING())
                    .column("email", DataTypes.STRING())
                    .column("created_at", DataTypes.TIMESTAMP(3))
                    .watermark("created_at", "created_at - INTERVAL '5' SECOND")
                    .build())
                .option("topic", "customers")
                .option("properties.bootstrap.servers", "kafka:29092")
                .option("properties.group.id", "customer-analytics")
                .option("scan.startup.mode", "latest-offset")
                .option("format", "json")
                .build());

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
