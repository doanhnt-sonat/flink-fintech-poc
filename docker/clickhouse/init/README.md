# ClickHouse Dashboard Infrastructure

## 📁 Files Overview

### **Core Files (Required)**
1. **`01-create-database.sql`** - Database setup
2. **`06-dashboard-tables.sql`** - Dashboard tables for 3 core panels
3. **`07-dashboard-pipelines.sql`** - Kafka pipelines from Flink
4. **`08-dashboard-queries.sql`** - Dashboard queries and views
5. **`10-dashboard-setup.sql`** - Complete setup script

## 🎯 3 Core Dashboard Panels

### **1. 🚨 Fraud & Security Panel**
- **Data Source**: `fintech.customer_fraud_alerts`
- **Purpose**: Real-time fraud detection monitoring
- **Key Metrics**:
  - Total fraud alerts
  - Alerts by severity (Critical, High, Medium, Low)
  - Fraud types distribution
  - High-risk transactions
  - Affected customers
  - Timeline trends

### **2. 🏪 Merchant Performance Panel**
- **Data Source**: `fintech.merchant_performance`
- **Purpose**: Merchant business performance analysis
- **Key Metrics**:
  - Revenue and transaction volume
  - Top performing merchants
  - Performance by country
  - Business type analysis
  - Risk assessment
  - MCC code performance

### **3. 👥 Customer Analytics Panel**
- **Data Source**: `fintech.customer_lifecycle` + `fintech.customer_transaction_metrics`
- **Purpose**: Customer behavior and lifecycle analysis
- **Key Metrics**:
  - Customer tier distribution
  - KYC status overview
  - Lifecycle events (upgrades/downgrades)
  - Risk score distribution
  - Transaction patterns
  - Geographic and device patterns

## 🚀 Quick Start

### **1. Setup Database**
```sql
-- Run in order:
source 01-create-database.sql
source 06-dashboard-tables.sql
source 07-dashboard-pipelines.sql
source 08-dashboard-queries.sql
source 10-dashboard-setup.sql
```

### **2. Verify Setup**
```sql
-- Check all tables exist
SELECT name FROM system.tables WHERE database = 'fintech_dashboard';

-- Check dashboard views
SELECT * FROM fraud_alerts_overview;
SELECT * FROM merchant_performance_overview;
SELECT * FROM customer_analytics_overview;
```

## 📊 Key Dashboard Views

### **Fraud & Security**
- `fraud_alerts_overview` - Today's fraud summary
- `fraud_alerts_by_severity` - Alerts by severity level
- `high_risk_transactions` - Critical/high risk transactions
- `fraud_alerts_timeline` - 24-hour timeline

### **Merchant Performance**
- `merchant_performance_overview` - Today's merchant summary
- `top_performing_merchants` - Top 20 merchants by revenue
- `merchant_performance_by_country` - Performance by country
- `business_type_performance` - Performance by business type

### **Customer Analytics**
- `customer_analytics_overview` - Today's customer summary
- `customer_tier_distribution` - Customers by tier
- `kyc_status_overview` - KYC status distribution
- `transaction_patterns` - Transaction type patterns

## 🔄 Real-time Monitoring

### **Current Hour Metrics**
```sql
SELECT * FROM realtime_fraud_monitoring;
SELECT * FROM realtime_merchant_monitoring;
SELECT * FROM realtime_customer_monitoring;
```

### **Trend Analysis**
```sql
SELECT * FROM fraud_alerts_trend;        -- Last 7 days
SELECT * FROM merchant_performance_trend; -- Last 7 days
SELECT * FROM customer_analytics_trend;   -- Last 7 days
```

## 📈 Data Flow

```
Flink Job → Kafka Topics → ClickHouse Pipelines → Dashboard Tables → Materialized Views → Dashboard Views
```

### **Kafka Topics**
- `fintech.customer_fraud_alerts` → Fraud & Security
- `fintech.merchant_performance` → Merchant Performance
- `fintech.customer_lifecycle` → Customer Analytics
- `fintech.customer_transaction_metrics` → Customer Analytics

## 🎨 Dashboard Implementation

### **Panel 1: Fraud & Security**
```sql
-- Main overview
SELECT * FROM fraud_alerts_overview;

-- Severity breakdown
SELECT * FROM fraud_alerts_by_severity;

-- High-risk transactions
SELECT * FROM high_risk_transactions LIMIT 20;
```

### **Panel 2: Merchant Performance**
```sql
-- Main overview
SELECT * FROM merchant_performance_overview;

-- Top merchants
SELECT * FROM top_performing_merchants LIMIT 20;

-- Country performance
SELECT * FROM merchant_performance_by_country;
```

### **Panel 3: Customer Analytics**
```sql
-- Main overview
SELECT * FROM customer_analytics_overview;

-- Tier distribution
SELECT * FROM customer_tier_distribution;

-- Transaction patterns
SELECT * FROM transaction_patterns;
```

## ⚡ Performance Tips

1. **Always filter by date** for better performance
2. **Use LIMIT** for large result sets
3. **Leverage materialized views** for complex aggregations
4. **Monitor table sizes** and partition counts
5. **Use appropriate data types** (Decimal64 for monetary values)

## 🔧 Maintenance

### **Check Data Flow**
```sql
-- Verify data is flowing
SELECT count() FROM fraud_detection_alerts WHERE date = today();
SELECT count() FROM merchant_performance_analytics WHERE date = today();
SELECT count() FROM customer_lifecycle_analytics WHERE date = today();
```

### **Monitor Performance**
```sql
-- Check table sizes
SELECT table, formatReadableSize(sum(bytes)) as size
FROM system.parts 
WHERE database = 'fintech_dashboard'
GROUP BY table;
```

## 📝 Notes

- All tables are **partitioned by date** for efficient time-based queries
- **Materialized views** provide real-time aggregations
- **Indexes** are created on frequently queried columns
- Data is updated **in real-time** from Flink streams
- Use **30-second refresh intervals** for dashboard updates
