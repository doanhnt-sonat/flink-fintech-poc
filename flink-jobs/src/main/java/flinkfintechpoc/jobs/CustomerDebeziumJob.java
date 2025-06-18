package flinkfintechpoc.jobs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Flink job that reads data from Kafka topic (customers.public.customers) with Debezium JSON format
 * and prints the results.
 */
public class CustomerDebeziumJob {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("customers.public.customers")
                .setGroupId("customer-debezium-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create a DataStream from the Kafka source
        DataStream<String> customerStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Debezium Source"
        );

        // Print the results
        customerStream.print("Customer Debezium Event: ");

        // Execute the job
        env.execute("Customer Debezium Job");
    }
}
