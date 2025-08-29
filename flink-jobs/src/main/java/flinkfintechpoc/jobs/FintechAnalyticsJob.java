package flinkfintechpoc.jobs;

import flinkfintechpoc.models.*;
import flinkfintechpoc.deserializers.*;
import flinkfintechpoc.processors.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * Flink Job for Real-time Fintech Analytics
 * 
 * This job processes streaming data from Kafka topics and provides:
 * - Real-time transaction processing
 * - Fraud detection
 * - Risk analytics
 * - Customer behavior analysis
 * - Real-time metrics for Grafana dashboard
 */
public class FintechAnalyticsJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(FintechAnalyticsJob.class);
    
    // Kafka topics
    private static final String TRANSACTIONS_TOPIC = "fintech.transactions";
    private static final String CUSTOMERS_TOPIC = "fintech.customers";
    private static final String FRAUD_ALERTS_TOPIC = "fintech.fraud_alerts";
    private static final String EVENTS_TOPIC = "fintech.events";
    
    // Output topics for Grafana
    private static final String REAL_TIME_METRICS_TOPIC = "fintech.realtime_metrics";
    private static final String FRAUD_DETECTION_TOPIC = "fintech.fraud_detection";
    private static final String CUSTOMER_ANALYTICS_TOPIC = "fintech.customer_analytics";
    
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(10000);
        
        // Create Kafka sources
        KafkaSource<String> transactionSource = createKafkaSource(TRANSACTIONS_TOPIC);
        KafkaSource<String> customerSource = createKafkaSource(CUSTOMERS_TOPIC);
        KafkaSource<String> fraudSource = createKafkaSource(FRAUD_ALERTS_TOPIC);
        KafkaSource<String> eventsSource = createKafkaSource(EVENTS_TOPIC);
        
        // Create data streams
        DataStream<Transaction> transactionStream = env
            .fromSource(transactionSource, WatermarkStrategy.noWatermarks(), "Transactions")
            .map(new TransactionDeserializer())
            .name("Deserialize Transactions");
            
        DataStream<Customer> customerStream = env
            .fromSource(customerSource, WatermarkStrategy.noWatermarks(), "Customers")
            .map(new CustomerDeserializer())
            .name("Deserialize Customers");
            
        DataStream<FraudAlert> fraudStream = env
            .fromSource(fraudSource, WatermarkStrategy.noWatermarks(), "Fraud Alerts")
            .map(new FraudAlertDeserializer())
            .name("Deserialize Fraud Alerts");
            
        DataStream<OutboxEvent> eventsStream = env
            .fromSource(eventsSource, WatermarkStrategy.noWatermarks(), "Events")
            .map(new EventDeserializer())
            .name("Deserialize Events");
        
        // 1. Real-time Transaction Processing with Fraud Detection
        DataStream<FraudDetectionResult> fraudDetectionStream = transactionStream
            .keyBy(Transaction::getCustomerId)
            .map(new FraudDetectionFunction())
            .name("Fraud Detection");
        
        // 2. Real-time Transaction Analytics
        DataStream<TransactionMetrics> transactionMetricsStream = transactionStream
            .keyBy(Transaction::getCustomerId)
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .process(new TransactionMetricsProcessor())
            .name("Transaction Metrics");
        
        // 3. Customer Behavior Analysis
        DataStream<CustomerBehaviorMetrics> customerBehaviorStream = transactionStream
            .keyBy(Transaction::getCustomerId)
            .window(TumblingEventTimeWindows.of(Time.minutes(15)))
            .process(new CustomerBehaviorProcessor())
            .name("Customer Behavior Analysis");
        
        // 4. Risk Analytics
        DataStream<RiskAnalytics> riskAnalyticsStream = transactionStream
            .keyBy(Transaction::getCustomerId)
            .window(TumblingEventTimeWindows.of(Time.minutes(10)))
            .process(new RiskAnalyticsProcessor())
            .name("Risk Analytics");
        
        // 5. Real-time Dashboard Metrics
        DataStream<DashboardMetrics> dashboardMetricsStream = transactionStream
            .keyBy(Transaction::getTransactionType)
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .process(new DashboardMetricsProcessor())
            .name("Dashboard Metrics");
        
        // 6. Anomaly Detection
        DataStream<AnomalyAlert> anomalyStream = transactionStream
            .keyBy(Transaction::getCustomerId)
            .map(new AnomalyDetectionFunction())
            .name("Anomaly Detection");
        
        // 7. Geographic Transaction Analysis
        DataStream<GeographicMetrics> geographicStream = transactionStream
            .filter(t -> t.getTransactionLocation() != null)
            .keyBy(t -> t.getTransactionLocation().get("country"))
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .process(new GeographicMetricsProcessor())
            .name("Geographic Analysis");
        
        // 8. Merchant Analysis
        DataStream<MerchantMetrics> merchantStream = transactionStream
            .filter(t -> t.getMerchantId() != null)
            .keyBy(Transaction::getMerchantId)
            .window(TumblingEventTimeWindows.of(Time.minutes(10)))
            .process(new MerchantMetricsProcessor())
            .name("Merchant Analysis");
        
        // 9. Time-based Pattern Analysis
        DataStream<TimePatternMetrics> timePatternStream = transactionStream
            .keyBy(t -> LocalDateTime.ofInstant(
                Instant.ofEpochMilli(t.getCreatedAt().getTime()), 
                ZoneId.systemDefault()
            ).getHour())
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .process(new TimePatternProcessor())
            .name("Time Pattern Analysis");
        
        // 10. Compliance Monitoring
        DataStream<ComplianceMetrics> complianceStream = transactionStream
            .filter(t -> !t.getComplianceFlags().isEmpty())
            .keyBy(Transaction::getCustomerId)
            .window(TumblingEventTimeWindows.of(Time.minutes(30)))
            .process(new ComplianceProcessor())
            .name("Compliance Monitoring");
        
        // Sink to external systems for Grafana
        // You can add JDBC sink, Elasticsearch sink, or other sinks here
        
        // Execute the job
        env.execute("Fintech Real-time Analytics Job");
    }
    
    private static KafkaSource<String> createKafkaSource(String topic) {
        return KafkaSource.<String>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics(topic)
            .setGroupId("flink-fintech-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
    }
}
