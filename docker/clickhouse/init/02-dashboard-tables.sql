-- =====================================================
-- DASHBOARD TABLES FOR 3 CORE PANELS
-- =====================================================
-- Based on Flink job analysis:
-- 1. Fraud & Security: fintech.customer_fraud_alerts
-- 2. Merchant Performance: fintech.merchant_performance  
-- 3. Customer Analytics: fintech.customer_lifecycle + fintech.customer_transaction_metrics

-- =====================================================
-- 1. FRAUD & SECURITY TABLES
-- =====================================================

-- Fraud Detection Alerts Table
CREATE TABLE IF NOT EXISTS fraud_detection_alerts (
    transactionId String,
    customerId String,
    fraudTypes Array(String),      -- List<String> as Array
    riskScore Float64,
    detectionTime DateTime64(3),
    alertType String,
    fraudDetails String,           -- JSON string for Map<String, Object>
    isFraudulent Boolean,
    severity String,
    recommendation String,
    -- Partitioning and indexing
    date Date MATERIALIZED toDate(detectionTime),
    hour UInt8 MATERIALIZED toHour(detectionTime)
) ENGINE = MergeTree()
PARTITION BY date
ORDER BY (customerId, detectionTime)
SETTINGS index_granularity = 8192;

-- =====================================================
-- 2. MERCHANT PERFORMANCE TABLES
-- =====================================================

-- Merchant Performance Analytics Table
CREATE TABLE IF NOT EXISTS merchant_performance_analytics (
    merchantId String,
    eventType String,
    transactionCount UInt32,
    totalAmount Float64,
    averageAmount Float64,
    eventTime DateTime64(3),
    performanceLevel String,
    riskLevel String,
    merchantName String,
    businessType String,
    mccCode String,
    country String,
    isActive Boolean,
    -- Partitioning and indexing
    date Date MATERIALIZED toDate(eventTime),
    hour UInt8 MATERIALIZED toHour(eventTime)
) ENGINE = ReplacingMergeTree(eventTime)
PARTITION BY date
ORDER BY (merchantId)
SETTINGS index_granularity = 8192;

-- =====================================================
-- 3. CUSTOMER ANALYTICS TABLES
-- =====================================================

-- Customer Lifecycle Analytics Table
CREATE TABLE IF NOT EXISTS customer_lifecycle_analytics (
    customerId String,
    eventType String,
    currentTier String,
    currentKycStatus String,
    eventTime DateTime64(3),
    tierChangeCount UInt32,
    isUpgrade Boolean,
    isDowngrade Boolean,
    kycCompleted Boolean,
    riskScore Float64,
    timeSinceLastUpdate UInt64,
    previousTier String,
    previousKycStatus String,
    riskScoreChange Float64,
    -- Partitioning and indexing
    date Date MATERIALIZED toDate(eventTime),
    hour UInt8 MATERIALIZED toHour(eventTime)
) ENGINE = ReplacingMergeTree(eventTime)
PARTITION BY date
ORDER BY (customerId)
SETTINGS index_granularity = 8192;

-- Customer Transaction Metrics Table
CREATE TABLE IF NOT EXISTS customer_transaction_metrics (
    customerId String,
    eventTime DateTime64(3),
    totalAmount Decimal64(2),
    transactionCount UInt32,
    averageAmount Decimal64(2),
    minAmount Decimal64(2),
    maxAmount Decimal64(2),
    preferredTransactionType String,
    preferredLocation String,
    preferredDevice String,
    transactionVelocity Float64,
    riskScore Float64,
    transactionTypeCounts Map(String, UInt32),  -- Map<String, Integer>
    locationCounts Map(String, UInt32),         -- Map<String, Integer>
    deviceCounts Map(String, UInt32),           -- Map<String, Integer>
    recentTransactionCount UInt32,
    lastTransactionAmount Decimal64(2),
    lastTransactionType String,
    -- Partitioning and indexing
    date Date MATERIALIZED toDate(eventTime),
    hour UInt8 MATERIALIZED toHour(eventTime)
) ENGINE = MergeTree()
PARTITION BY date
ORDER BY (customerId, eventTime)
SETTINGS index_granularity = 8192;

-- =====================================================
-- MATERIALIZED VIEWS FOR REAL-TIME AGGREGATIONS
-- =====================================================

-- 1. Fraud Alerts Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS fraud_alerts_summary
ENGINE = SummingMergeTree()
PARTITION BY date
ORDER BY (date, severity)
AS SELECT
    date,
    severity,
    count() as alertCount,
    avg(riskScore) as avgRiskScore,
    uniq(customerId) as affectedCustomers,
    uniq(transactionId) as affectedTransactions
FROM fraud_detection_alerts
GROUP BY date, severity;

-- 2. Merchant Performance Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS merchant_performance_summary
ENGINE = SummingMergeTree()
PARTITION BY date
ORDER BY (date, country, businessType)
AS SELECT
    date,
    country,
    businessType,
    sum(totalAmount) as totalRevenue,
    sum(transactionCount) as totalTransactions,
    avg(averageAmount) as avgTransactionValue,
    count() as merchantCount,
    sumIf(transactionCount, performanceLevel = 'HIGH_PERFORMANCE') as highPerformingMerchants
FROM merchant_performance_analytics
GROUP BY date, country, businessType;

-- 3. Customer Lifecycle Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS customer_lifecycle_summary
ENGINE = SummingMergeTree()
PARTITION BY date
ORDER BY (date, currentTier, currentKycStatus)
AS SELECT
    date,
    currentTier,
    currentKycStatus,
    count() as customerCount,
    avg(riskScore) as avgRiskScore,
    sum(tierChangeCount) as totalTierChanges,
    sumIf(1, isUpgrade = true) as upgrades,
    sumIf(1, isDowngrade = true) as downgrades,
    sumIf(1, kycCompleted = true) as kycCompletions
FROM customer_lifecycle_analytics
GROUP BY date, currentTier, currentKycStatus;

-- 4. Customer Transaction Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS customer_transaction_summary
ENGINE = SummingMergeTree()
PARTITION BY date
ORDER BY (date, preferredTransactionType)
AS SELECT
    date,
    preferredTransactionType,
    sum(totalAmount) as totalAmount,
    sum(transactionCount) as transactionCount,
    avg(averageAmount) as avgAmount,
    uniq(customerId) as uniqueCustomers,
    avg(riskScore) as avgRiskScore
FROM customer_transaction_metrics
GROUP BY date, preferredTransactionType;

-- =====================================================
-- DASHBOARD OPTIMIZED VIEWS
-- =====================================================

-- 1. Fraud & Security Dashboard
CREATE VIEW IF NOT EXISTS fraud_security_dashboard AS
SELECT
    toString(max(date)) as period,
    count() as totalAlerts,
    uniq(customerId) as affectedCustomers,
    avg(riskScore) as avgRiskScore,
    countIf(severity = 'CRITICAL') as criticalAlerts,
    countIf(severity = 'HIGH') as highAlerts,
    countIf(severity = 'MEDIUM') as mediumAlerts,
    countIf(severity = 'LOW') as lowAlerts
FROM fraud_detection_alerts
WHERE date = (SELECT max(date) FROM fraud_detection_alerts);

-- 2. Merchant Performance Dashboard
CREATE VIEW IF NOT EXISTS merchant_performance_dashboard AS
SELECT
    toString(max(date)) as period,
    country,
    businessType,
    count() as merchantCount,
    sum(totalAmount) as totalRevenue,
    sum(transactionCount) as totalTransactions,
    avg(averageAmount) as avgTransactionValue,
    sumIf(transactionCount, performanceLevel = 'HIGH_PERFORMANCE') as highPerformingTransactions,
    sumIf(transactionCount, performanceLevel = 'HIGH_PERFORMANCE') * 100.0 / sum(transactionCount) as performanceRate
FROM merchant_performance_analytics
WHERE date = (SELECT max(date) FROM merchant_performance_analytics)
GROUP BY country, businessType
ORDER BY totalRevenue DESC;

-- 3. Customer Analytics Dashboard
CREATE VIEW IF NOT EXISTS customer_analytics_dashboard AS
SELECT
    toString(max(date)) as period,
    currentTier,
    currentKycStatus,
    count() as customerCount,
    avg(riskScore) as avgRiskScore,
    sum(tierChangeCount) as totalTierChanges,
    sumIf(1, isUpgrade = true) as upgrades,
    sumIf(1, isDowngrade = true) as downgrades,
    sumIf(1, kycCompleted = true) as kycCompletions,
    count() * 100.0 / sum(count()) OVER() as percentage
FROM customer_lifecycle_analytics
WHERE date = (SELECT max(date) FROM customer_lifecycle_analytics)
GROUP BY currentTier, currentKycStatus
ORDER BY customerCount DESC;

-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================

-- Fraud alerts indexes
ALTER TABLE fraud_detection_alerts ADD INDEX idx_severity severity TYPE set(0) GRANULARITY 1;
ALTER TABLE fraud_detection_alerts ADD INDEX idx_fraudulent isFraudulent TYPE set(0) GRANULARITY 1;
ALTER TABLE fraud_detection_alerts ADD INDEX idx_alert_type alertType TYPE set(0) GRANULARITY 1;

-- Merchant analytics indexes
ALTER TABLE merchant_performance_analytics ADD INDEX idx_country country TYPE set(0) GRANULARITY 1;
ALTER TABLE merchant_performance_analytics ADD INDEX idx_business businessType TYPE set(0) GRANULARITY 1;
ALTER TABLE merchant_performance_analytics ADD INDEX idx_performance performanceLevel TYPE set(0) GRANULARITY 1;
ALTER TABLE merchant_performance_analytics ADD INDEX idx_risk riskLevel TYPE set(0) GRANULARITY 1;

-- Customer analytics indexes
ALTER TABLE customer_lifecycle_analytics ADD INDEX idx_tier currentTier TYPE set(0) GRANULARITY 1;
ALTER TABLE customer_lifecycle_analytics ADD INDEX idx_kyc currentKycStatus TYPE set(0) GRANULARITY 1;
ALTER TABLE customer_lifecycle_analytics ADD INDEX idx_event eventType TYPE set(0) GRANULARITY 1;

-- Transaction metrics indexes
ALTER TABLE customer_transaction_metrics ADD INDEX idx_transaction_type preferredTransactionType TYPE set(0) GRANULARITY 1;
ALTER TABLE customer_transaction_metrics ADD INDEX idx_location preferredLocation TYPE set(0) GRANULARITY 1;
ALTER TABLE customer_transaction_metrics ADD INDEX idx_device preferredDevice TYPE set(0) GRANULARITY 1;
