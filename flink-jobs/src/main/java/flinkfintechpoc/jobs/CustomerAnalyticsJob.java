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

        // Define a mock source table for customers using the datagen connector
        tableEnv.createTable("mock_customers", 
            TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                    .column("id", DataTypes.STRING())
                    .column("name", DataTypes.STRING())
                    .column("email", DataTypes.STRING())
                    .column("created_at", DataTypes.TIMESTAMP(3))
                    .build())
                .option("rows-per-second", "5")
                .build());

        // Create a view with a processing time attribute
        tableEnv.executeSql(
            "CREATE VIEW customers_with_proctime AS " +
            "SELECT *, PROCTIME() AS proc_time " +
            "FROM mock_customers"
        );

        // SQL query to count customers per minute using processing time
        String sql = 
            "SELECT " +
            "  TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS window_start, " +
            "  COUNT(*) AS customer_count " +
            "FROM customers_with_proctime " +
            "GROUP BY TUMBLE(proc_time, INTERVAL '1' MINUTE)";


        // Execute the SQL query
        Table resultTable = tableEnv.sqlQuery(sql);

        // Convert the Table back to a DataStream
        DataStream<Tuple2<Long, Long>> resultStream = 
            tableEnv.toDataStream(resultTable)
                .map(row -> {
                    java.time.LocalDateTime localDateTime = (java.time.LocalDateTime) row.getField(0);
                    Long count = (Long) row.getField(1);
                    // Convert LocalDateTime to milliseconds since epoch
                    long timestamp = localDateTime.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
                    return new Tuple2<>(timestamp, count);
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
