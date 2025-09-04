-- Create views optimized for Grafana dashboards

USE fintech_analytics;

-- 1. Real-time Transaction Summary View (from customer_transaction_metrics)
CREATE VIEW IF NOT EXISTS realtime_transaction_summary AS
SELECT
	toStartOfMinute(eventTime) AS minute,
	sum(transactionCount) AS transaction_count,
	sum(totalAmount) AS total_amount,
	avg(averageAmount) AS avg_amount,
	countDistinct(customerId) AS unique_customers
FROM customer_transaction_metrics
WHERE eventTime >= now() - INTERVAL 1 HOUR
GROUP BY minute
ORDER BY minute;

-- 2. Customer Lifecycle Overview View (by currentTier)
CREATE VIEW IF NOT EXISTS customer_lifecycle_overview AS
SELECT
	currentTier,
	count() AS customer_count,
	avg(tierChangeCount) AS avg_tier_changes,
	avg(riskScore) AS avg_risk_score
FROM customer_lifecycle_metrics
WHERE eventTime >= now() - INTERVAL 24 HOUR
GROUP BY currentTier
ORDER BY customer_count DESC;

-- 3. Merchant Performance Overview View
CREATE VIEW IF NOT EXISTS merchant_performance_overview AS
SELECT
	businessType,
	countDistinct(merchantId) AS merchant_count,
	sum(transactionCount) AS total_transactions,
	sum(totalAmount) AS total_revenue,
	avg(averageAmount) AS avg_average_amount
FROM merchant_performance_metrics
WHERE eventTime >= now() - INTERVAL 24 HOUR
GROUP BY businessType
ORDER BY total_revenue DESC;

-- 4. Fraud Detection Summary View
CREATE VIEW IF NOT EXISTS fraud_detection_summary AS
SELECT
	toStartOfHour(detectionTime) AS hour,
	count() AS total_alerts,
	countIf(severity = 'HIGH') AS high_risk_alerts,
	countIf(severity = 'MEDIUM') AS medium_risk_alerts,
	countIf(severity = 'LOW') AS low_risk_alerts,
	avg(riskScore) AS avg_fraud_score
FROM customer_fraud_alerts
WHERE detectionTime >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour;

-- 5. Transaction Metrics by Time View
CREATE VIEW IF NOT EXISTS transaction_metrics_by_time AS
SELECT
	toStartOfMinute(eventTime) AS minute,
	count() AS window_count,
	sum(transactionCount) AS total_transactions,
	sum(totalAmount) AS total_amount,
	avg(transactionVelocity) AS avg_velocity_score
FROM customer_transaction_metrics
WHERE eventTime >= now() - INTERVAL 1 HOUR
GROUP BY minute
ORDER BY minute;

-- 6. Customer Risk Analysis View
CREATE VIEW IF NOT EXISTS customer_risk_analysis AS
SELECT
	CASE 
		WHEN riskScore >= 0.8 THEN 'Very High'
		WHEN riskScore >= 0.6 THEN 'High'
		WHEN riskScore >= 0.4 THEN 'Medium'
		WHEN riskScore >= 0.2 THEN 'Low'
		ELSE 'Very Low'
	END AS risk_category,
	count() AS customer_count,
	avg(tierChangeCount) AS avg_tier_changes,
	avg(riskScore) AS avg_risk_score
FROM customer_lifecycle_metrics
WHERE eventTime >= now() - INTERVAL 24 HOUR
GROUP BY risk_category
ORDER BY customer_count DESC;

-- 7. Hourly Transaction Volume View
CREATE VIEW IF NOT EXISTS hourly_transaction_volume AS
SELECT
	toHour(eventTime) AS hour_of_day,
	sum(transactionCount) AS transaction_count,
	sum(totalAmount) AS total_amount,
	avg(averageAmount) AS avg_amount
FROM customer_transaction_metrics
WHERE eventTime >= now() - INTERVAL 7 DAY
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- 8. Top Merchants by Revenue View
CREATE VIEW IF NOT EXISTS top_merchants_by_revenue AS
SELECT
	merchantId,
	businessType,
	totalAmount,
	transactionCount,
	performanceLevel
FROM merchant_performance_metrics
WHERE eventTime >= now() - INTERVAL 24 HOUR
ORDER BY totalAmount DESC
LIMIT 20;

-- 9. Customer Activity by Preferred Transaction Type
CREATE VIEW IF NOT EXISTS customer_activity_by_tx_type AS
SELECT
	preferredTransactionType,
	count() AS activity_count,
	avg(transactionVelocity) AS avg_velocity,
	avg(riskScore) AS avg_risk
FROM customer_transaction_metrics
WHERE eventTime >= now() - INTERVAL 24 HOUR
GROUP BY preferredTransactionType
ORDER BY activity_count DESC;

-- 10. Real-time Dashboard Metrics View
CREATE VIEW IF NOT EXISTS realtime_dashboard_metrics AS
SELECT
	now() AS current_time,
	(SELECT sum(transactionCount) FROM customer_transaction_metrics WHERE eventTime >= now() - INTERVAL 1 HOUR) AS transactions_last_hour,
	(SELECT sum(totalAmount) FROM customer_transaction_metrics WHERE eventTime >= now() - INTERVAL 1 HOUR) AS revenue_last_hour,
	(SELECT countIf(isFraudulent) FROM customer_fraud_alerts WHERE detectionTime >= now() - INTERVAL 1 HOUR) AS active_fraud_alerts,
	(SELECT countDistinct(customerId) FROM customer_transaction_metrics WHERE eventTime >= now() - INTERVAL 1 HOUR) AS active_customers_last_hour;
