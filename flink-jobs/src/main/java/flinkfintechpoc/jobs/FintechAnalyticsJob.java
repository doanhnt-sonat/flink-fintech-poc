package flinkfintechpoc.jobs;

import flinkfintechpoc.models.*;
import flinkfintechpoc.processors.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import java.util.Properties;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import java.time.Duration;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink Job for Real-time Fintech Analytics - 4 Core Processors
 * 
 * This job processes streaming data from Kafka topics and provides:
 * 1. Customer Lifecycle Analysis (Cascade Pattern: CustomerDataEnrichment + CustomerLifecycle)
 * 2. Merchant Performance Analysis (Merchant table) 
 * 3. Customer Transaction Metrics (Transaction table)
 * 4. Customer Fraud Detection (Transaction + Account tables)
 * 
 * Covers all major Flink techniques:
 * - State Management (ValueState, MapState, ListState)
 * - Event Time Processing (Watermarks, Event time windows)
 * - Complex Event Processing (CEP patterns)
 * - Broadcast State Pattern (Reference data enrichment)
 * - Windowed Processing (Tumbling, Sliding, Session windows)
 * - Custom Triggers and Functions
 * - Stream Joins and Side Inputs
 * - Cascade Pattern for Multi-step Enrichment
 */
public class FintechAnalyticsJob {
    private static final Logger LOG = LoggerFactory.getLogger(FintechAnalyticsJob.class);
    
    // Kafka topics
    private static final String TRANSACTIONS_TOPIC = "fintech.public.transactions";
    private static final String CUSTOMERS_TOPIC = "fintech.public.customers";
    private static final String MERCHANTS_TOPIC = "fintech.public.merchants";
    private static final String CUSTOMER_SESSIONS_TOPIC = "fintech.public.customer_sessions";
    private static final String ACCOUNTS_TOPIC = "fintech.public.accounts";
    
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(10000);
        
        // Create Kafka sources (JSON → POJO via JsonDeserializationSchema)
        KafkaSource<Transaction> transactionSource = createJsonKafkaSource(TRANSACTIONS_TOPIC, Transaction.class);
        KafkaSource<Customer> customerSource = createJsonKafkaSource(CUSTOMERS_TOPIC, Customer.class);
        KafkaSource<Merchant> merchantSource = createJsonKafkaSource(MERCHANTS_TOPIC, Merchant.class);
        KafkaSource<CustomerSession> customerSessionSource = createJsonKafkaSource(CUSTOMER_SESSIONS_TOPIC, CustomerSession.class);
        KafkaSource<Account> accountSource = createJsonKafkaSource(ACCOUNTS_TOPIC, Account.class);

        // Create data streams (no extra map step needed)
        DataStream<Transaction> transactionStream = env
            .fromSource(transactionSource, WatermarkStrategy.noWatermarks(), "Transactions");

        // Log sample of incoming transactions to verify data flow
        DataStream<Transaction> transactionStreamLogged = transactionStream
            .map(t -> {
                if (Math.random() < 0.5) {
                    LOG.info("RX Transaction id={}, customerId={}, merchantId={}, amount={}",
                            t.getId(), t.getCustomerId(), t.getMerchantId(), t.getAmount());
                }
                return t;
            })
            .name("Log Transactions");
        
        DataStream<Customer> customerStream = env
            .fromSource(customerSource, WatermarkStrategy.noWatermarks(), "Customers");
        
        DataStream<Merchant> merchantStream = env
            .fromSource(merchantSource, WatermarkStrategy.noWatermarks(), "Merchants");
        
        DataStream<CustomerSession> customerSessionStream = env
            .fromSource(customerSessionSource, WatermarkStrategy.noWatermarks(), "Customer Sessions");
        
        DataStream<Account> accountStream = env
            .fromSource(accountSource, WatermarkStrategy.noWatermarks(), "Accounts");
        
        // ============================================================================
        // 4 CORE PROCESSORS - Covering All Flink Techniques
        // ============================================================================
        
        // Prepare filtered streams to avoid null keys
        DataStream<Transaction> transactionsByCustomer = transactionStreamLogged
            .filter(t -> t.getCustomerId() != null)
            .name("Filter Transactions with customerId");
        DataStream<Transaction> transactionsByMerchant = transactionStreamLogged
            .filter(t -> t.getMerchantId() != null)
            .name("Filter Transactions with merchantId");

        // 1. CASCADE PATTERN - Customer Lifecycle Analysis
        // Step 1: Transaction + Customer → EnrichedTransaction
        DataStream<EnrichedTransaction> enrichedTransactionStream = transactionsByCustomer
            .keyBy(Transaction::getCustomerId)
            .connect(customerStream.broadcast(CustomerDataEnrichmentProcessor.CUSTOMER_STATE_DESCRIPTOR))
            .process(new CustomerDataEnrichmentProcessor())
            .name("Customer Data Enrichment");
        
        // Step 2: EnrichedTransaction + CustomerSession → CustomerLifecycleMetrics
        DataStream<CustomerLifecycleMetrics> customerLifecycleStream = enrichedTransactionStream
            .keyBy(EnrichedTransaction::getCustomerId)
            .connect(customerSessionStream.broadcast(CustomerLifecycleProcessor.SESSION_STATE_DESCRIPTOR))
            .process(new CustomerLifecycleProcessor())
            .name("Customer Lifecycle Analysis");
        
        // 2. MERCHANT PERFORMANCE PROCESSOR - Transaction + Merchant data
        // Techniques: Broadcast State Pattern, Side Inputs, Windowed Aggregations
        DataStream<MerchantAnalyticsMetrics> merchantPerformanceStream = transactionsByMerchant
            .keyBy(Transaction::getMerchantId)
            .connect(merchantStream.broadcast(MerchantPerformanceProcessor.MERCHANT_STATE_DESCRIPTOR))
            .process(new MerchantPerformanceProcessor())
            .name("Merchant Performance Analysis");
        
        // 3. CUSTOMER TRANSACTION METRICS PROCESSOR - Transaction table
        // Techniques: Windowed Processing, State Management, Custom Triggers
        DataStream<TransactionMetrics> customerTransactionMetricsStream = transactionsByCustomer
            .keyBy(Transaction::getCustomerId)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(30)))
            .process(new CustomerTransactionMetricsProcessor())
            .name("Customer Transaction Metrics Analysis");
        
        // 4. CUSTOMER FRAUD DETECTION PROCESSOR - Transaction + Account data (Fraud Detection)
        // Techniques: Complex Event Processing, State Management, Stream Joins simulation
        DataStream<FraudDetectionResult> customerFraudDetectionStream = transactionsByCustomer
            .keyBy(Transaction::getCustomerId)
            .connect(accountStream.broadcast(CustomerFraudDetectionProcessor.ACCOUNT_STATE_DESCRIPTOR))
            .process(new CustomerFraudDetectionProcessor())
            .filter(result -> result != null && result.isFraudulent())
            .name("Customer Fraud Detection");
        
        // ============================================================================
        // OUTPUT STREAMS - Analytics Results
        // ============================================================================
        
        // 1. Customer Lifecycle Metrics - Customer analytics & lifecycle events
        customerLifecycleStream
            .map(metrics -> metrics.toString())
            .sinkTo(createKafkaSink("fintech.customer_lifecycle"))
            .name("Customer Lifecycle Output");
        
        // 2. Merchant Performance - Merchant performance & business insights
        merchantPerformanceStream
            .map(metrics -> metrics.toString())
            .sinkTo(createKafkaSink("fintech.merchant_performance"))
            .name("Merchant Performance Output");
        
        // 3. Customer Transaction Metrics - Core transaction analytics & dashboard metrics
        customerTransactionMetricsStream
            .map(metrics -> metrics.toString())
            .sinkTo(createKafkaSink("fintech.customer_transaction_metrics"))
            .name("Customer Transaction Metrics Output");
        
        // 4. Customer Fraud Detection Alerts - Real-time security monitoring
        customerFraudDetectionStream
            .map(alert -> alert.toString())
            .sinkTo(createKafkaSink("fintech.customer_fraud_alerts"))
            .name("Customer Fraud Detection Output");
        
        // Execute the job
        env.execute("Fintech Real-time Analytics Job - 4 Core Processors");
    }
    
    
    private static <T> KafkaSource<T> createJsonKafkaSource(String topic, Class<T> cls) {
        return KafkaSource.<T>builder()
            .setBootstrapServers("kafka:29092")
            .setTopics(topic)
            .setGroupId("flink-analytics-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new JsonDeserializationSchema<>(cls))
            .build();
    }
    
    private static KafkaSink<String> createKafkaSink(String topic) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("transaction.timeout.ms", "300000"); // 5 minutes

        return KafkaSink.<String>builder()
            .setBootstrapServers("kafka:29092")
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(topic)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build()
            )
            .setKafkaProducerConfig(kafkaProps)
            .setTransactionalIdPrefix(topic + "-tx")
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .build();
    }
}