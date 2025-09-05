-- =====================================================
-- DATA PIPELINES FROM KAFKA TO DASHBOARD TABLES
-- =====================================================
-- Based on Flink job analysis for 3 core panels

-- =====================================================
-- 1. FRAUD & SECURITY PIPELINE
-- =====================================================

-- Fraud Detection Alerts Pipeline
CREATE TABLE IF NOT EXISTS fraud_detection_alerts_pipeline (
    transactionId String,
    customerId String,
    fraudTypes Array(String),  -- List<String> as Array
    riskScore Float64,
    detectionTime String,  -- JSON string from Java Date
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

-- =====================================================
-- 2. MERCHANT PERFORMANCE PIPELINE
-- =====================================================

-- Merchant Performance Pipeline
CREATE TABLE IF NOT EXISTS merchant_performance_pipeline (
    merchantId String,
    eventType String,
    transactionCount UInt32,
    totalAmount Float64,
    averageAmount Float64,
    eventTime String,  -- JSON string from Java Date
    performanceLevel String,
    riskLevel String,
    merchantName String,
    businessType String,
    mccCode String,
    country String,
    isActive Boolean
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'fintech.merchant_performance',
    kafka_group_name = 'clickhouse_merchant_performance_group',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 1,
    kafka_max_block_size = 1048576;

-- =====================================================
-- 3. CUSTOMER ANALYTICS PIPELINES
-- =====================================================

-- Customer Lifecycle Pipeline
CREATE TABLE IF NOT EXISTS customer_lifecycle_pipeline (
    customerId String,
    eventType String,
    currentTier String,
    currentKycStatus String,
    eventTime String,  -- JSON string from Java Date
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

-- Customer Transaction Metrics Pipeline
CREATE TABLE IF NOT EXISTS customer_transaction_metrics_pipeline (
    customerId String,
    eventTime String,  -- JSON string from Java Date
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
    transactionTypeCounts Map(String, UInt32),  -- Map<String, Integer>
    locationCounts Map(String, UInt32),  -- Map<String, Integer>
    deviceCounts Map(String, UInt32),  -- Map<String, Integer>
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

-- =====================================================
-- MATERIALIZED VIEWS FOR DATA TRANSFORMATION
-- =====================================================

-- 1. Fraud Detection Alerts Data Transformer
CREATE MATERIALIZED VIEW IF NOT EXISTS fraud_detection_alerts_mv
TO fraud_detection_alerts
AS SELECT
    transactionId,
    customerId,
    fraudTypes,  -- Array maps directly
    riskScore,
    parseDateTime64BestEffort(detectionTime) as detectionTime,
    alertType,
    fraudDetails,
    isFraudulent,
    severity,
    recommendation
FROM fraud_detection_alerts_pipeline;

-- 2. Merchant Performance Data Transformer
CREATE MATERIALIZED VIEW IF NOT EXISTS merchant_performance_mv
TO merchant_performance_analytics
AS SELECT
    merchantId,
    eventType,
    transactionCount,
    totalAmount,
    averageAmount,
    parseDateTime64BestEffort(eventTime) as eventTime,
    performanceLevel,
    riskLevel,
    merchantName,
    businessType,
    mccCode,
    country,
    isActive
FROM merchant_performance_pipeline;

-- 3. Customer Lifecycle Data Transformer
CREATE MATERIALIZED VIEW IF NOT EXISTS customer_lifecycle_mv
TO customer_lifecycle_analytics
AS SELECT
    customerId,
    eventType,
    currentTier,
    currentKycStatus,
    parseDateTime64BestEffort(eventTime) as eventTime,
    tierChangeCount,
    isUpgrade,
    isDowngrade,
    kycCompleted,
    riskScore,
    timeSinceLastUpdate,
    previousTier,
    previousKycStatus,
    riskScoreChange
FROM customer_lifecycle_pipeline;

-- 4. Customer Transaction Metrics Data Transformer
CREATE MATERIALIZED VIEW IF NOT EXISTS customer_transaction_metrics_mv
TO customer_transaction_metrics
AS SELECT
    customerId,
    parseDateTime64BestEffort(eventTime) as eventTime,
    toDecimal64(totalAmount, 2) as totalAmount,
    transactionCount,
    toDecimal64(averageAmount, 2) as averageAmount,
    toDecimal64(minAmount, 2) as minAmount,
    toDecimal64(maxAmount, 2) as maxAmount,
    preferredTransactionType,
    preferredLocation,
    preferredDevice,
    transactionVelocity,
    riskScore,
    transactionTypeCounts,
    locationCounts,
    deviceCounts,
    recentTransactionCount,
    toDecimal64(lastTransactionAmount, 2) as lastTransactionAmount,
    lastTransactionType
FROM customer_transaction_metrics_pipeline;
