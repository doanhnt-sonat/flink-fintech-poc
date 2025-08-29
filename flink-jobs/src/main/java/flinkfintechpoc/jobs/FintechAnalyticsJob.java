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
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
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
    private static final String ACCOUNTS_TOPIC = "fintech.accounts";
    private static final String MERCHANTS_TOPIC = "fintech.merchants";
    private static final String CUSTOMER_SESSIONS_TOPIC = "fintech.customer_sessions";
    private static final String EVENTS_TOPIC = "fintech.events";
    
    // Output topics for Grafana
    private static final String REAL_TIME_METRICS_TOPIC = "fintech.realtime_metrics";
    private static final String CUSTOMER_ANALYTICS_TOPIC = "fintech.customer_analytics";
    private static final String TRANSACTION_ANALYTICS_TOPIC = "fintech.transaction_analytics";
    
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(10000);
        
        // Create Kafka sources
        KafkaSource<String> transactionSource = createKafkaSource(TRANSACTIONS_TOPIC);
        KafkaSource<String> customerSource = createKafkaSource(CUSTOMERS_TOPIC);
        KafkaSource<String> accountSource = createKafkaSource(ACCOUNTS_TOPIC);
        KafkaSource<String> merchantSource = createKafkaSource(MERCHANTS_TOPIC);
        KafkaSource<String> customerSessionSource = createKafkaSource(CUSTOMER_SESSIONS_TOPIC);
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
            
        DataStream<Account> accountStream = env
            .fromSource(accountSource, WatermarkStrategy.noWatermarks(), "Accounts")
            .map(new AccountDeserializer())
            .name("Deserialize Accounts");
            
        DataStream<Merchant> merchantStream = env
            .fromSource(merchantSource, WatermarkStrategy.noWatermarks(), "Merchants")
            .map(new MerchantDeserializer())
            .name("Deserialize Merchants");
            
        DataStream<CustomerSession> customerSessionStream = env
            .fromSource(customerSessionSource, WatermarkStrategy.noWatermarks(), "Customer Sessions")
            .map(new CustomerSessionDeserializer())
            .name("Deserialize Customer Sessions");
            
        DataStream<OutboxEvent> eventsStream = env
            .fromSource(eventsSource, WatermarkStrategy.noWatermarks(), "Events")
            .map(new EventDeserializer())
            .name("Deserialize Events");
        
        // 1. Real-time Transaction Processing and Analytics
        // Transaction stream is ready for processing
        
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
        
        // 6. Fraud Detection and Anomaly Detection
        DataStream<AnomalyAlert> fraudDetectionStream = transactionStream
            .keyBy(Transaction::getCustomerId)
            .map(new FraudDetectionFunction())
            .filter(alert -> alert != null)  // Filter out null alerts
            .name("Fraud Detection");
        
        // 7. Geographic Transaction Analysis
        DataStream<GeographicMetrics> geographicStream = transactionStream
            .filter(t -> t.getTransactionLocation() != null)
            .keyBy(t -> t.getTransactionLocation().get("country"))
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .process(new GeographicMetricsProcessor())
            .name("Geographic Analysis");
        
        // 8. Merchant Analysis
        DataStream<MerchantMetrics> merchantMetricsStream = transactionStream
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
        
        // Sink to Kafka topics for Grafana Dashboard
        // Fraud Detection Alerts
        fraudDetectionStream
            .map(alert -> alert.toString())  // Convert to string for Kafka
            .sinkTo(createKafkaSink("fintech.fraud_alerts"))
            .name("Fraud Alerts Sink");
        
        // Compliance Metrics
        complianceStream
            .map(metrics -> metrics.toString())  // Convert to string for Kafka
            .sinkTo(createKafkaSink("fintech.compliance_metrics"))
            .name("Compliance Metrics Sink");
        
        // Transaction Metrics
        transactionMetricsStream
            .map(metrics -> metrics.toString())  // Convert to string for Kafka
            .sinkTo(createKafkaSink("fintech.transaction_metrics"))
            .name("Transaction Metrics Sink");
        
        // Customer Behavior Metrics
        customerBehaviorStream
            .map(metrics -> metrics.toString())  // Convert to string for Kafka
            .sinkTo(createKafkaSink("fintech.customer_behavior"))
            .name("Customer Behavior Sink");
        
        // Risk Analytics
        riskAnalyticsStream
            .map(analytics -> analytics.toString())  // Convert to string for Kafka
            .sinkTo(createKafkaSink("fintech.risk_analytics"))
            .name("Risk Analytics Sink");
        
        // Dashboard Metrics
        dashboardMetricsStream
            .map(metrics -> metrics.toString())  // Convert to string for Kafka
            .sinkTo(createKafkaSink("fintech.dashboard_metrics"))
            .name("Dashboard Metrics Sink");
        
        // Geographic Metrics
        geographicStream
            .map(metrics -> metrics.toString())  // Convert to string for Kafka
            .sinkTo(createKafkaSink("fintech.geographic_metrics"))
            .name("Geographic Metrics Sink");
        
        // Merchant Metrics
        merchantMetricsStream
            .map(metrics -> metrics.toString())  // Convert to string for Kafka
            .sinkTo(createKafkaSink("fintech.merchant_metrics"))
            .name("Merchant Metrics Sink");
        
        // Time Pattern Metrics
        timePatternStream
            .map(metrics -> metrics.toString())  // Convert to string for Kafka
            .sinkTo(createKafkaSink("fintech.time_pattern_metrics"))
            .name("Time Pattern Metrics Sink");
        
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
    
    private static KafkaSink<String> createKafkaSink(String topic) {
        return KafkaSink.<String>builder()
            .setBootstrapServers("localhost:9092")
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(topic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .build();
    }
}
