package flinkfintechpoc.jobs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple test job to verify Kafka connectivity and data flow
 */
public class KafkaTestJob {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTestJob.class);
    
    public static void main(String[] args) throws Exception {
        LOG.info("Starting Kafka Test Job...");
        
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Create a simple Kafka source that reads raw strings
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:29092")
            .setTopics("fintech.public.transactions")
            .setGroupId("flink-test-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        
        LOG.info("Created Kafka source for topic: fintech.public.transactions");
        
        // Create data stream
        DataStream<String> stream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Test Source");
        
        // Log all incoming messages
        stream
            .map(message -> {
                LOG.info("Received message: {}", message);
                return message;
            })
            .name("Log Messages")
            .print()
            .name("Print Messages");
        
        // Execute the job
        LOG.info("Executing Kafka Test Job...");
        env.execute("Kafka Test Job");
    }
}
