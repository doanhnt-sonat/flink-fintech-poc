-- Create materialized views to automatically move data from Kafka tables to regular tables

USE fintech_analytics;

-- 1. Materialized View for Customer Lifecycle Metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS customer_lifecycle_mv TO customer_lifecycle_metrics AS
SELECT
    customerId,
    eventType,
    currentTier,
    currentKycStatus,
    parseDateTimeBestEffort(eventTime) as eventTime,
    tierChangeCount,
    isUpgrade,
    isDowngrade,
    kycCompleted,
    riskScore,
    timeSinceLastUpdate,
    previousTier,
    previousKycStatus,
    riskScoreChange,
    now() as processed_at
FROM customer_lifecycle_kafka;

-- 1b. MV to maintain latest customer lifecycle snapshot (ReplacingMergeTree)
CREATE MATERIALIZED VIEW IF NOT EXISTS customer_lifecycle_current_mv
TO fintech_analytics.customer_lifecycle_current AS
SELECT
    customerId,
    eventType,
    currentTier,
    currentKycStatus,
    parseDateTimeBestEffort(eventTime) as eventTime,
    tierChangeCount,
    toUInt8(isUpgrade)   AS isUpgrade,
    toUInt8(isDowngrade) AS isDowngrade,
    toUInt8(kycCompleted) AS kycCompleted,
    riskScore,
    timeSinceLastUpdate,
    previousTier,
    previousKycStatus,
    riskScoreChange,
    now() as processed_at
FROM customer_lifecycle_kafka;

-- 2. Materialized View for Merchant Performance Metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS merchant_performance_mv TO merchant_performance_metrics AS
SELECT
    merchantId,
    eventType,
    transactionCount,
    totalAmount,
    averageAmount,
    parseDateTimeBestEffort(eventTime) as eventTime,
    performanceLevel,
    riskLevel,
    businessType,
    mccCode,
    isActive,
    now() as processed_at
FROM merchant_performance_kafka;

-- 3. Materialized View for Customer Transaction Metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS customer_transaction_metrics_mv TO customer_transaction_metrics AS
SELECT
    customerId,
    parseDateTimeBestEffort(eventTime) as eventTime,
    toFloat64OrZero(totalAmount) as totalAmount,
    transactionCount,
    toFloat64OrZero(averageAmount) as averageAmount,
    toFloat64OrZero(minAmount) as minAmount,
    toFloat64OrZero(maxAmount) as maxAmount,
    preferredTransactionType,
    preferredLocation,
    preferredDevice,
    transactionVelocity,
    riskScore,
    transactionTypeCounts,
    locationCounts,
    deviceCounts,
    recentTransactionCount,
    toFloat64OrZero(lastTransactionAmount) as lastTransactionAmount,
    lastTransactionType,
    now() as processed_at
FROM customer_transaction_metrics_kafka;

-- 4. Materialized View for Customer Fraud Alerts
CREATE MATERIALIZED VIEW IF NOT EXISTS customer_fraud_alerts_mv TO customer_fraud_alerts AS
SELECT
    transactionId,
    customerId,
    fraudTypes,
    riskScore,
    parseDateTimeBestEffort(detectionTime) as detectionTime,
    alertType,
    fraudDetails,
    isFraudulent,
    severity,
    recommendation,
    now() as processed_at
FROM customer_fraud_alerts_kafka;

-- Note: Only processing Flink analytics output, not raw data
-- Raw data processing is handled by Flink before reaching ClickHouse
