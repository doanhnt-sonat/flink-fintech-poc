-- Create tables for fintech analytics data from Kafka topics

USE fintech_analytics;

-- 1. Customer Lifecycle Metrics Table
CREATE TABLE IF NOT EXISTS customer_lifecycle_metrics (
    customerId String,
    eventType String,
    currentTier String,
    currentKycStatus String,
    eventTime DateTime,
    tierChangeCount UInt32,
    isUpgrade Boolean,
    isDowngrade Boolean,
    kycCompleted Boolean,
    riskScore Float64,
    timeSinceLastUpdate UInt64,
    previousTier String,
    previousKycStatus String,
    riskScoreChange Float64,
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (customerId, eventTime)
PARTITION BY toYYYYMM(eventTime);

-- 1b. Customer Lifecycle Current-State Table (keep only latest row per customer)
CREATE TABLE IF NOT EXISTS customer_lifecycle_current (
    customerId String,
    eventType String,
    currentTier String,
    currentKycStatus String,
    eventTime DateTime,
    tierChangeCount UInt32,
    isUpgrade UInt8,
    isDowngrade UInt8,
    kycCompleted UInt8,
    riskScore Float64,
    timeSinceLastUpdate UInt64,
    previousTier String,
    previousKycStatus String,
    riskScoreChange Float64,
    processed_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(processed_at)
ORDER BY customerId;

-- 2. Merchant Performance Metrics Table
CREATE TABLE IF NOT EXISTS merchant_performance_metrics (
    merchantId String,
    eventType String,
    transactionCount UInt32,
    totalAmount Float64,
    averageAmount Float64,
    eventTime DateTime,
    performanceLevel String,
    riskLevel String,
    businessType String,
    mccCode String,
    isActive Boolean,
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (merchantId, eventTime)
PARTITION BY toYYYYMM(eventTime);

-- 3. Customer Transaction Metrics Table
CREATE TABLE IF NOT EXISTS customer_transaction_metrics (
    customerId String,
    eventTime DateTime,
    totalAmount Float64,
    transactionCount UInt32,
    averageAmount Float64,
    minAmount Float64,
    maxAmount Float64,
    preferredTransactionType String,
    preferredLocation String,
    preferredDevice String,
    transactionVelocity Float64,
    riskScore Float64,
    transactionTypeCounts String,  -- Map<String, Integer> as JSON
    locationCounts String,  -- Map<String, Integer> as JSON
    deviceCounts String,  -- Map<String, Integer> as JSON
    recentTransactionCount UInt32,
    lastTransactionAmount Float64,
    lastTransactionType String,
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (customerId, eventTime)
PARTITION BY toYYYYMM(eventTime);

-- 4. Customer Fraud Alerts Table
CREATE TABLE IF NOT EXISTS customer_fraud_alerts (
    transactionId String,
    customerId String,
    fraudTypes String,  -- List<String> as JSON
    riskScore Float64,
    detectionTime DateTime,
    alertType String,
    fraudDetails String,  -- Map<String, Object> as JSON
    isFraudulent Boolean,
    severity String,
    recommendation String,
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (customerId, detectionTime)
PARTITION BY toYYYYMM(detectionTime);


