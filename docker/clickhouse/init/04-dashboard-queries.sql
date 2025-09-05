-- =====================================================
-- DASHBOARD QUERIES FOR 3 CORE PANELS
-- =====================================================
-- Based on Flink job analysis:
-- 1. Fraud & Security: fintech.customer_fraud_alerts
-- 2. Merchant Performance: fintech.merchant_performance  
-- 3. Customer Analytics: fintech.customer_lifecycle + fintech.customer_transaction_metrics

-- =====================================================
-- 1. FRAUD & SECURITY PANEL QUERIES
-- =====================================================

-- 1.1 Fraud Alerts Overview (Today)
CREATE VIEW IF NOT EXISTS fraud_alerts_overview AS
SELECT
    'Today' as period,
    count() as totalAlerts,
    uniq(customerId) as affectedCustomers,
    uniq(transactionId) as affectedTransactions,
    avg(riskScore) as avgRiskScore,
    max(riskScore) as maxRiskScore,
    countIf(severity = 'CRITICAL') as criticalAlerts,
    countIf(severity = 'HIGH') as highAlerts,
    countIf(severity = 'MEDIUM') as mediumAlerts,
    countIf(severity = 'LOW') as lowAlerts
FROM fraud_detection_alerts
WHERE date = (SELECT max(date) FROM fraud_detection_alerts);

-- 1.2 Fraud Alerts by Severity (Today)
CREATE VIEW IF NOT EXISTS fraud_alerts_by_severity AS
SELECT
    severity,
    count() as alertCount,
    avg(riskScore) as avgRiskScore,
    uniq(customerId) as affectedCustomers,
    uniq(transactionId) as affectedTransactions,
    count() * 100.0 / sum(count()) OVER() as percentage
FROM fraud_detection_alerts
WHERE date = (SELECT max(date) FROM fraud_detection_alerts)
GROUP BY severity
ORDER BY alertCount DESC;

-- 1.3 Fraud Types Distribution (Today)
CREATE VIEW IF NOT EXISTS fraud_types_distribution AS
SELECT
    fraudTypes[1] as primaryFraudType,
    count() as alertCount,
    avg(riskScore) as avgRiskScore,
    uniq(customerId) as affectedCustomers,
    count() * 100.0 / sum(count()) OVER() as percentage
FROM fraud_detection_alerts
WHERE date = (SELECT max(date) FROM fraud_detection_alerts)
GROUP BY primaryFraudType
ORDER BY alertCount DESC;

-- 1.4 High-Risk Transactions (Today)
CREATE VIEW IF NOT EXISTS high_risk_transactions AS
SELECT
    transactionId,
    customerId,
    riskScore,
    severity,
    alertType,
    recommendation,
    detectionTime
FROM fraud_detection_alerts
WHERE date = (SELECT max(date) FROM fraud_detection_alerts) AND severity IN ('CRITICAL', 'HIGH')
ORDER BY riskScore DESC
LIMIT 50;

-- 1.5 Fraud Alerts Timeline (Last 24 Hours)
CREATE VIEW IF NOT EXISTS fraud_alerts_timeline AS
SELECT
    toStartOfHour(detectionTime) as hour,
    count() as alertCount,
    avg(riskScore) as avgRiskScore,
    uniq(customerId) as affectedCustomers,
    countIf(severity = 'CRITICAL') as criticalAlerts
FROM fraud_detection_alerts
WHERE date >= (SELECT addDays(max(date), -1) FROM fraud_detection_alerts)
GROUP BY hour
ORDER BY hour;

-- 1.6 Top Affected Customers (Today)
CREATE VIEW IF NOT EXISTS top_affected_customers AS
SELECT
    customerId,
    count() as alertCount,
    avg(riskScore) as avgRiskScore,
    max(riskScore) as maxRiskScore,
    uniq(transactionId) as affectedTransactions,
    groupArray(severity) as severities
FROM fraud_detection_alerts
WHERE date = today()
GROUP BY customerId
ORDER BY alertCount DESC
LIMIT 20;

-- =====================================================
-- 2. MERCHANT PERFORMANCE PANEL QUERIES
-- =====================================================

-- 2.1 Merchant Performance Overview (Today)
CREATE VIEW IF NOT EXISTS merchant_performance_overview AS
SELECT
    'Today' as period,
    count() as totalMerchants,
    sum(totalAmount) as totalRevenue,
    sum(transactionCount) as totalTransactions,
    avg(averageAmount) as avgTransactionValue,
    sumIf(transactionCount, performanceLevel = 'HIGH_PERFORMANCE') as highPerformingTransactions,
    sumIf(transactionCount, performanceLevel = 'HIGH_PERFORMANCE') * 100.0 / sum(transactionCount) as performanceRate
FROM merchant_performance_analytics
WHERE date = (SELECT max(date) FROM merchant_performance_analytics);

-- 2.2 Top Performing Merchants (Today)
CREATE VIEW IF NOT EXISTS top_performing_merchants AS
SELECT
    merchantName,
    country,
    businessType,
    totalAmount as revenue,
    transactionCount,
    averageAmount,
    performanceLevel,
    riskLevel,
    mccCode
FROM merchant_performance_analytics
WHERE date = (SELECT max(date) FROM merchant_performance_analytics)
ORDER BY totalAmount DESC
LIMIT 20;

-- 2.3 Merchant Performance by Country (Today)
CREATE VIEW IF NOT EXISTS merchant_performance_by_country AS
SELECT
    country,
    count() as merchantCount,
    sum(totalAmount) as totalRevenue,
    sum(transactionCount) as totalTransactions,
    avg(averageAmount) as avgTransactionValue,
    sumIf(transactionCount, performanceLevel = 'HIGH_PERFORMANCE') as highPerformingTransactions,
    sumIf(transactionCount, performanceLevel = 'HIGH_PERFORMANCE') * 100.0 / sum(transactionCount) as performanceRate
FROM merchant_performance_analytics
WHERE date = (SELECT max(date) FROM merchant_performance_analytics)
GROUP BY country
ORDER BY totalRevenue DESC;

-- 2.4 Business Type Performance (Today)
CREATE VIEW IF NOT EXISTS business_type_performance AS
SELECT
    businessType,
    count() as merchantCount,
    sum(totalAmount) as totalRevenue,
    sum(transactionCount) as totalTransactions,
    avg(averageAmount) as avgTransactionValue,
    sumIf(transactionCount, performanceLevel = 'HIGH_PERFORMANCE') as highPerformingTransactions,
    sumIf(transactionCount, performanceLevel = 'HIGH_PERFORMANCE') * 100.0 / sum(transactionCount) as performanceRate
FROM merchant_performance_analytics
WHERE date = (SELECT max(date) FROM merchant_performance_analytics)
GROUP BY businessType
ORDER BY totalRevenue DESC;

-- 2.5 Merchant Risk Analysis (Today)
CREATE VIEW IF NOT EXISTS merchant_risk_analysis AS
SELECT
    riskLevel,
    count() as merchantCount,
    avg(totalAmount) as avgRevenue,
    avg(transactionCount) as avgTransactions,
    avg(averageAmount) as avgTransactionValue,
    count() * 100.0 / sum(count()) OVER() as percentage
FROM merchant_performance_analytics
WHERE date = (SELECT max(date) FROM merchant_performance_analytics)
GROUP BY riskLevel
ORDER BY merchantCount DESC;

-- 2.6 MCC Code Performance (Today)
CREATE VIEW IF NOT EXISTS mcc_code_performance AS
SELECT
    mccCode,
    businessType,
    count() as merchantCount,
    sum(totalAmount) as totalRevenue,
    sum(transactionCount) as totalTransactions,
    avg(averageAmount) as avgTransactionValue
FROM merchant_performance_analytics
WHERE date = (SELECT max(date) FROM merchant_performance_analytics)
GROUP BY mccCode, businessType
ORDER BY totalRevenue DESC
LIMIT 20;

-- =====================================================
-- 3. CUSTOMER ANALYTICS PANEL QUERIES
-- =====================================================

-- 3.1 Customer Analytics Overview (Today)
CREATE VIEW IF NOT EXISTS customer_analytics_overview AS
SELECT
    'Today' as period,
    uniq(customerId) as totalCustomers,
    avg(riskScore) as avgRiskScore,
    sum(tierChangeCount) as totalTierChanges,
    sumIf(1, isUpgrade = true) as upgrades,
    sumIf(1, isDowngrade = true) as downgrades,
    sumIf(1, kycCompleted = true) as kycCompletions
FROM customer_lifecycle_analytics
WHERE date = (SELECT max(date) FROM customer_lifecycle_analytics);

-- 3.2 Customer Tier Distribution (Today)
CREATE VIEW IF NOT EXISTS customer_tier_distribution AS
SELECT
    currentTier,
    count() as customerCount,
    avg(riskScore) as avgRiskScore,
    sum(tierChangeCount) as totalTierChanges,
    sumIf(1, isUpgrade = true) as upgrades,
    sumIf(1, isDowngrade = true) as downgrades,
    count() * 100.0 / sum(count()) OVER() as percentage
FROM customer_lifecycle_analytics
WHERE date = (SELECT max(date) FROM customer_lifecycle_analytics)
GROUP BY currentTier
ORDER BY customerCount DESC;

-- 3.3 KYC Status Overview (Today)
CREATE VIEW IF NOT EXISTS kyc_status_overview AS
SELECT
    currentKycStatus,
    count() as customerCount,
    avg(riskScore) as avgRiskScore,
    count() * 100.0 / sum(count()) OVER() as percentage
FROM customer_lifecycle_analytics
WHERE date = (SELECT max(date) FROM customer_lifecycle_analytics)
GROUP BY currentKycStatus
ORDER BY customerCount DESC;

-- 3.4 Customer Lifecycle Events (Today)
CREATE VIEW IF NOT EXISTS customer_lifecycle_events AS
SELECT
    eventType,
    currentTier,
    previousTier,
    count() as eventCount,
    avg(riskScore) as avgRiskScore,
    uniq(customerId) as uniqueCustomers
FROM customer_lifecycle_analytics
WHERE date = (SELECT max(date) FROM customer_lifecycle_analytics) AND (isUpgrade = true OR isDowngrade = true)
GROUP BY eventType, currentTier, previousTier
ORDER BY eventCount DESC;

-- 3.5 Customer Risk Distribution (Today)
CREATE VIEW IF NOT EXISTS customer_risk_distribution AS
SELECT
    currentTier,
    currentKycStatus,
    count() as customerCount,
    avg(riskScore) as avgRiskScore,
    min(riskScore) as minRiskScore,
    max(riskScore) as maxRiskScore
FROM customer_lifecycle_analytics
WHERE date = (SELECT max(date) FROM customer_lifecycle_analytics)
GROUP BY currentTier, currentKycStatus
ORDER BY avgRiskScore DESC;

-- 3.6 Transaction Patterns (Today)
CREATE VIEW IF NOT EXISTS transaction_patterns AS
SELECT
    preferredTransactionType,
    count() as transactionCount,
    sum(totalAmount) as totalAmount,
    avg(averageAmount) as avgAmount,
    uniq(customerId) as uniqueCustomers,
    avg(riskScore) as avgRiskScore
FROM customer_transaction_metrics
WHERE date = (SELECT max(date) FROM customer_transaction_metrics)
GROUP BY preferredTransactionType
ORDER BY transactionCount DESC;

-- 3.7 Customer Geographic Distribution (Today)
CREATE VIEW IF NOT EXISTS customer_geographic_distribution AS
SELECT
    preferredLocation,
    count() as transactionCount,
    sum(totalAmount) as totalAmount,
    uniq(customerId) as uniqueCustomers,
    avg(riskScore) as avgRiskScore
FROM customer_transaction_metrics
WHERE date = (SELECT max(date) FROM customer_transaction_metrics)
GROUP BY preferredLocation
ORDER BY transactionCount DESC
LIMIT 20;

-- 3.8 Device Usage Patterns (Today)
CREATE VIEW IF NOT EXISTS device_usage_patterns AS
SELECT
    preferredDevice,
    count() as transactionCount,
    sum(totalAmount) as totalAmount,
    uniq(customerId) as uniqueCustomers,
    avg(riskScore) as avgRiskScore
FROM customer_transaction_metrics
WHERE date = (SELECT max(date) FROM customer_transaction_metrics)
GROUP BY preferredDevice
ORDER BY transactionCount DESC;

-- =====================================================
-- 4. REAL-TIME MONITORING QUERIES
-- =====================================================

-- 4.1 Real-time Fraud Monitoring (Current Hour)
CREATE VIEW IF NOT EXISTS realtime_fraud_monitoring AS
SELECT
    'Current Hour' as period,
    count() as alertCount,
    avg(riskScore) as avgRiskScore,
    uniq(customerId) as affectedCustomers,
    countIf(severity = 'CRITICAL') as criticalAlerts
FROM fraud_detection_alerts
WHERE (date, hour) = (
  SELECT max(tuple(date, hour)) FROM fraud_detection_alerts
);

-- 4.2 Real-time Merchant Monitoring (Current Hour)
CREATE VIEW IF NOT EXISTS realtime_merchant_monitoring AS
SELECT
    'Current Hour' as period,
    count() as merchantCount,
    sum(totalAmount) as totalRevenue,
    sum(transactionCount) as totalTransactions,
    avg(averageAmount) as avgTransactionValue
FROM merchant_performance_analytics
WHERE (date, hour) = (
  SELECT max(tuple(date, hour)) FROM merchant_performance_analytics
);

-- 4.3 Real-time Customer Monitoring (Current Hour)
CREATE VIEW IF NOT EXISTS realtime_customer_monitoring AS
SELECT
    'Current Hour' as period,
    uniq(customerId) as activeCustomers,
    avg(riskScore) as avgRiskScore,
    sum(tierChangeCount) as tierChanges
FROM customer_lifecycle_analytics
WHERE (date, hour) = (
  SELECT max(tuple(date, hour)) FROM customer_lifecycle_analytics
);

-- =====================================================
-- 5. TREND ANALYSIS QUERIES
-- =====================================================

-- 5.1 Fraud Alerts Trend (Last 7 Days)
CREATE VIEW IF NOT EXISTS fraud_alerts_trend AS
SELECT
    date,
    count() as dailyAlerts,
    avg(riskScore) as avgRiskScore,
    uniq(customerId) as affectedCustomers,
    countIf(severity = 'CRITICAL') as criticalAlerts
FROM fraud_detection_alerts
WHERE date >= today() - 7
GROUP BY date
ORDER BY date;

-- 5.2 Merchant Performance Trend (Last 7 Days)
CREATE VIEW IF NOT EXISTS merchant_performance_trend AS
SELECT
    date,
    count() as dailyMerchants,
    sum(totalAmount) as dailyRevenue,
    sum(transactionCount) as dailyTransactions,
    avg(averageAmount) as avgTransactionValue
FROM merchant_performance_analytics
WHERE date >= today() - 7
GROUP BY date
ORDER BY date;

-- 5.3 Customer Analytics Trend (Last 7 Days)
CREATE VIEW IF NOT EXISTS customer_analytics_trend AS
SELECT
    date,
    uniq(customerId) as dailyActiveCustomers,
    avg(riskScore) as avgRiskScore,
    sum(tierChangeCount) as dailyTierChanges,
    sumIf(1, isUpgrade = true) as dailyUpgrades,
    sumIf(1, isDowngrade = true) as dailyDowngrades
FROM customer_lifecycle_analytics
WHERE date >= today() - 7
GROUP BY date
ORDER BY date;
