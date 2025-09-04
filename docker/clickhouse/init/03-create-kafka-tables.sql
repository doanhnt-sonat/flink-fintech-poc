-- Create Kafka engine tables for consuming data from Kafka topics

USE fintech_analytics;

-- 1. Customer Lifecycle Metrics from Kafka
CREATE TABLE IF NOT EXISTS customer_lifecycle_kafka (
    customerId String,
    eventType String,
    currentTier String,
    currentKycStatus String,
    eventTime String,  -- Keep as String for JSON parsing
    tierChangeCount UInt32,
    isUpgrade Boolean,
    isDowngrade Boolean,
    kycCompleted Boolean,
    riskScore Float64,
    timeSinceLastUpdate UInt64,
    previousTier String,
    previousKycStatus String,
    riskScoreChange Float64
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'fintech.customer_lifecycle',
    kafka_group_name = 'clickhouse_customer_lifecycle_group',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 1,
    kafka_max_block_size = 1048576;

-- 2. Merchant Performance Metrics from Kafka
CREATE TABLE IF NOT EXISTS merchant_performance_kafka (
    merchantId String,
    eventType String,
    transactionCount UInt32,
    totalAmount Float64,
    averageAmount Float64,
    eventTime String,  -- Keep as String for JSON parsing
    performanceLevel String,
    riskLevel String,
    businessType String,
    mccCode String,
    isActive Boolean
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'fintech.merchant_performance',
    kafka_group_name = 'clickhouse_merchant_performance_group',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 1,
    kafka_max_block_size = 1048576;

-- 3. Customer Transaction Metrics from Kafka
CREATE TABLE IF NOT EXISTS customer_transaction_metrics_kafka (
    customerId String,
    eventTime String,  -- Keep as String for JSON parsing
    totalAmount String,  -- BigDecimal from Java
    transactionCount UInt32,
    averageAmount String,  -- BigDecimal from Java
    minAmount String,  -- BigDecimal from Java
    maxAmount String,  -- BigDecimal from Java
    preferredTransactionType String,
    preferredLocation String,
    preferredDevice String,
    transactionVelocity Float64,
    riskScore Float64,
    transactionTypeCounts String,  -- Map<String, Integer> as JSON
    locationCounts String,  -- Map<String, Integer> as JSON
    deviceCounts String,  -- Map<String, Integer> as JSON
    recentTransactionCount UInt32,
    lastTransactionAmount String,  -- BigDecimal from Java
    lastTransactionType String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'fintech.customer_transaction_metrics',
    kafka_group_name = 'clickhouse_customer_transaction_group',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 1,
    kafka_max_block_size = 1048576;

-- 4. Customer Fraud Alerts from Kafka
CREATE TABLE IF NOT EXISTS customer_fraud_alerts_kafka (
    transactionId String,
    customerId String,
    fraudTypes String,  -- List<String> as JSON
    riskScore Float64,
    detectionTime String,  -- Keep as String for JSON parsing
    alertType String,
    fraudDetails String,  -- Map<String, Object> as JSON
    isFraudulent Boolean,
    severity String,
    recommendation String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'fintech.customer_fraud_alerts',
    kafka_group_name = 'clickhouse_fraud_alerts_group',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 1,
    kafka_max_block_size = 1048576;

-- Note: Only consuming Flink output topics, not raw Debezium data
-- Flink processes raw data and outputs analytics results to these topics
