# Flink Fintech POC - Real-time Analytics Pipeline

Một hệ thống real-time analytics hoàn chỉnh cho fintech, sử dụng PostgreSQL, Kafka, Debezium, Apache Flink và Grafana để xử lý và phân tích dữ liệu tài chính theo thời gian thực.

## 📊 Tóm Tắt Luồng Dữ Liệu

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   app-python    │───▶│   PostgreSQL    │───▶│    Debezium     │───▶│      Kafka      │───▶│   Flink Jobs    │
│                 │    │                 │    │   (CDC)         │    │                 │    │                 │
│ Data Generator  │    │ Core Tables     │    │ Outbox Pattern  │    │ Topics          │    │ Real-time       │
│ + Producer      │    │ + Outbox Table  │    │ + Direct CDC    │    │ (10+ topics)    │    │ Processing      │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                                                              │
                                │                                                              ▼
                                │                                                    ┌─────────────────┐
                                │                                                    │     Grafana     │
                                │                                                    │   Dashboards    │
                                │                                                    │   Real-time     │
                                └────────────────────────────────────────────────────▶│  Visualization  │
                                                                                     └─────────────────┘
```

**Luồng dữ liệu chính:**
1. **Data Generation**: Python app tạo dữ liệu fintech realistic
2. **Database Storage**: Lưu vào PostgreSQL với outbox pattern
3. **CDC Streaming**: Debezium capture changes và stream ra Kafka
4. **Real-time Processing**: Flink xử lý dữ liệu theo thời gian thực
5. **Analytics Output**: Kết quả được gửi đến Grafana dashboard

## 🏗️ Kiến Trúc App-Python, Database và Data Generation

### **App-Python Architecture**

#### **Core Components:**
- **`main.py`**: Entry point và CLI commands
- **`realtime_producer.py`**: Producer chính tạo dữ liệu liên tục
- **`data_generator.py`**: Generator dữ liệu realistic với behavioral patterns
- **`models.py`**: Pydantic models định nghĩa data structures
- **`connectors.py`**: Database connections và Debezium connector setup
- **`config.py`**: Configuration settings

#### **Data Generation Strategy:**
```python
# Realistic behavioral patterns
transaction_patterns = {
    CustomerTier.BASIC: {'daily_txns': (1, 5), 'avg_amount': (10, 500)},
    CustomerTier.PREMIUM: {'daily_txns': (3, 15), 'avg_amount': (50, 2000)},
    CustomerTier.VIP: {'daily_txns': (5, 25), 'avg_amount': (100, 10000)},
    CustomerTier.ENTERPRISE: {'daily_txns': (10, 100), 'avg_amount': (1000, 100000)}
}
```

### **Database Design**

#### **Core Business Tables:**
| Bảng | Mục đích | Dữ liệu chính |
|------|----------|----------------|
| `customers` | Quản lý khách hàng | Thông tin cá nhân, tier, risk_score, credit_score |
| `accounts` | Quản lý tài khoản | Loại tài khoản, số dư, credit_limit, interest_rate |
| `transactions` | Giao dịch tài chính | Loại giao dịch, số tiền, merchant, risk_score, compliance_flags |
| `merchants` | Thông tin thương nhân | Tên công ty, loại hình, MCC codes, địa chỉ |
| `fraud_alerts` | Cảnh báo gian lận | Loại cảnh báo, severity, confidence_score, rules_triggered |

#### **Analytics Tables:**
| Bảng | Mục đích | Dữ liệu chính |
|------|----------|----------------|
| `account_balances` | Biến động số dư | Số dư theo thời gian, pending_debits |
| `customer_sessions` | Phiên làm việc | Channel, device, IP, location, actions_count |
| `market_data` | Dữ liệu thị trường | Symbol, price, change, volume, market_cap |

#### **Outbox Pattern Table:**
| Bảng | Mục đích | Dữ liệu chính |
|------|----------|----------------|
| `outbox` | Business events | aggregate_type, aggregate_id, event_type, payload (JSON) |

### **Data Generation Process**

#### **1. Initial Data Generation:**
```python
# Generate realistic customer base
scenario_data = self.data_generator.generate_realistic_scenario(self.num_customers)
# Creates interconnected customers, accounts, merchants
```

#### **2. Real-time Data Production:**
```python
# Continuous transaction generation
async def _transaction_generator_loop(self):
    while self.is_running:
        await self._generate_and_send_transaction()
        await asyncio.sleep(interval)
```

#### **3. Outbox Pattern Implementation:**
```python
# Store transaction + outbox events atomically
await self._store_transaction(transaction)
await self._store_outbox_events(transaction, customer, account)
```

#### **4. Logic Tạo Data Fake (Thực Tế từ Code):**

##### **A) Customer Generation với Behavioral Patterns:**
```python
# Customer tier distribution với behavioral characteristics
customer_tiers = {
    'BASIC': 60%,      # 60% khách hàng basic
    'PREMIUM': 25%,    # 25% khách hàng premium  
    'VIP': 10%,        # 10% khách hàng VIP
    'ENTERPRISE': 5%   # 5% khách hàng enterprise
}

# Risk score calculation dựa trên tier
base_risk = {
    CustomerTier.BASIC: 20,
    CustomerTier.PREMIUM: 15,
    CustomerTier.VIP: 10,
    CustomerTier.ENTERPRISE: 5
}
risk_score = base_risk[tier] + random.gauss(0, 10)
```

##### **B) Transaction Generation với Realistic Patterns:**
```python
# Time-based transaction rates (theo giờ trong ngày)
def _get_time_based_transaction_rate(self) -> float:
    current_hour = datetime.now().hour
    
    if 9 <= current_hour <= 17:      # Business hours: 1.5x - 2.0x
        return self.production_rate * random.uniform(1.5, 2.0)
    elif 18 <= current_hour <= 22:   # Evening peak: 1.2x - 1.8x
        return self.production_rate * random.uniform(1.2, 1.8)
    elif 6 <= current_hour <= 8:     # Morning: 0.8x - 1.2x
        return self.production_rate * random.uniform(0.8, 1.2)
    else:                            # Night/early morning: 0.2x - 0.5x
        return self.production_rate * random.uniform(0.2, 0.5)

# Amount distribution theo customer tier
amount_patterns = {
    'BASIC': {'min': 10, 'max': 500, 'avg': 100},
    'PREMIUM': {'min': 50, 'max': 2000, 'avg': 500},
    'VIP': {'min': 100, 'max': 10000, 'avg': 2000},
    'ENTERPRISE': {'min': 1000, 'max': 100000, 'avg': 25000}
}
```

##### **C) Fraud Detection Logic (Thực Tế từ Code):**
```python
# Điều kiện tạo fraud alert (từ realtime_producer.py)
if transaction.risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL] and random.random() > 0.6:
    fraud_alert = self.data_generator.generate_fraud_alert(customer, transaction)

# Fraud alert generation (từ data_generator.py)
alert_types = [
    'Unusual spending pattern', 'Geographic anomaly', 'Velocity check failed',
    'Device mismatch', 'Time-based anomaly', 'Amount threshold exceeded',
    'Merchant blacklist match', 'Card testing detected'
]

# Random generation với weighted distribution
alert = FraudAlert(
    alert_type=random.choice(alert_types),  # Random chọn 1 type
    severity=random.choices([LOW, MEDIUM, HIGH, CRITICAL], weights=[10, 30, 40, 20])[0],
    confidence_score=random.uniform(0.5, 1.0),  # Random 0.5-1.0
    rules_triggered=[f"RULE_{random.randint(100, 999)}" for _ in range(random.randint(1, 3))]
)
```

##### **D) Compliance Flags Generation (Thực Tế từ Code):**
```python
def _generate_compliance_flags(self, risk_score: float, amount: Decimal) -> List[str]:
    flags = []
    
    if amount > Decimal('10000'):
        flags.append('HIGH_VALUE')
    
    if risk_score > 70:
        flags.append('HIGH_RISK')
    
    if amount > Decimal('3000') and random.random() > 0.8:
        flags.append('STRUCTURING_SUSPECTED')
    
    if random.random() > 0.95:
        flags.append('SANCTIONS_CHECK_REQUIRED')
    
    return flags
```

##### **E) Risk Scoring Logic (Thực Tế từ Code):**
```python
# Risk scoring trong generate_transaction
risk_factors = 0
if amount > Decimal('10000'):
    risk_factors += 20
if customer.risk_score > 50:
    risk_factors += 10
if transaction_type == TransactionType.WIRE_TRANSFER:
    risk_factors += 15

risk_score = min(100, risk_factors + random.uniform(0, 20))
risk_level = RiskLevel.CRITICAL if risk_score > 80 else \
            RiskLevel.HIGH if risk_score > 60 else \
            RiskLevel.MEDIUM if risk_score > 30 else RiskLevel.LOW
```

##### **F) Customer Selection Logic (Thực Tế từ Code):**
```python
def _select_customer_for_transaction(self) -> Customer:
    # Higher tier customers are more active
    weights = []
    for customer in self.customers:
        if customer.tier == CustomerTier.ENTERPRISE:
            weight = 4.0
        elif customer.tier == CustomerTier.VIP:
            weight = 3.0
        elif customer.tier == CustomerTier.PREMIUM:
            weight = 2.0
        else:
            weight = 1.0
        weights.append(weight)
    
    return random.choices(self.customers, weights=weights)[0]
```

## 🔌 Kafka Topics và Debezium Connectors

### **3 Debezium Connectors**

#### **1. Main Connector (`fintech-main-connector`):**
- **Bảng được monitor**: `customers`, `accounts`, `transactions`, `merchants`, `fraud_alerts`
- **Topics**: `fintech.customers`, `fintech.accounts`, `fintech.transactions`, `fintech.merchants`, `fintech.fraud_alerts`
- **Mục đích**: Stream trực tiếp thay đổi dữ liệu từ các bảng chính

#### **2. Outbox Connector (`fintech-outbox-connector`):**
- **Bảng được monitor**: `outbox`
- **Topics**: `fintech.events.*` (được route tự động theo event_type)
- **Transformation**: `io.debezium.transforms.outbox.EventRouter`
- **Mục đích**: Stream business events với context đầy đủ

#### **3. Analytics Connector (`fintech-analytics-connector`):**
- **Bảng được monitor**: `account_balances`, `customer_sessions`
- **Topics**: `fintech.analytics.*`
- **Mục đích**: Stream dữ liệu analytics và metrics

### **Kafka Topics Structure**

```
fintech.customers          # Customer data changes
fintech.accounts           # Account data changes  
fintech.transactions       # Transaction data changes
fintech.merchants          # Merchant data changes
fintech.fraud_alerts       # Fraud alert data changes
fintech.events.*           # Business events (routed by event_type)
fintech.analytics.*        # Analytics data
```

## ⚡ Flink Jobs - Real-time Processing

### **Main Analytics Job: `FintechAnalyticsJob`**

#### **Input Streams:**
- **Transaction Stream**: Giao dịch real-time từ `fintech.transactions`
- **Customer Stream**: Thông tin khách hàng từ `fintech.customers`
- **Fraud Alert Stream**: Cảnh báo gian lận từ `fintech.fraud_alerts`
- **Events Stream**: Business events từ `fintech.events`

#### **Processing Streams (10 Analytics Pipelines):**

| Stream | Input | Logic | Time Window | Output |
|--------|-------|-------|-------------|---------|
| **Fraud Detection** | Transactions | Phân tích giao dịch để phát hiện gian lận | Real-time | `FraudDetectionResult` |
| **Transaction Metrics** | Transactions | Metrics theo customer | 5 phút | `TransactionMetrics` |
| **Customer Behavior** | Transactions | Phân tích hành vi khách hàng | 15 phút | `CustomerBehaviorMetrics` |
| **Risk Analytics** | Transactions | Đánh giá rủi ro theo customer | 10 phút | `RiskAnalytics` |
| **Dashboard Metrics** | Transactions | Metrics tổng hợp cho dashboard | 1 phút | `DashboardMetrics` |
| **Anomaly Detection** | Transactions | Phát hiện bất thường | Real-time | `AnomalyAlert` |
| **Geographic Analysis** | Transactions | Phân tích theo địa lý | 5 phút | `GeographicMetrics` |
| **Merchant Analysis** | Transactions | Phân tích theo merchant | 10 phút | `MerchantMetrics` |
| **Time Pattern Analysis** | Transactions | Pattern theo giờ | 1 giờ | `TimePatternMetrics` |
| **Compliance Monitoring** | Transactions | Giám sát compliance | 30 phút | `ComplianceMetrics` |

#### **Logic Chi Tiết của Từng Flink Job:**

##### **1. Fraud Detection Job:**
```java
// Logic: Phân tích giao dịch real-time để phát hiện gian lận
public class FraudDetectionFunction extends RichMapFunction<Transaction, FraudDetectionResult> {
    
    // State: Lưu trữ pattern của từng customer
    private ValueState<CustomerPatternState> customerPatternState;
    
    public FraudDetectionResult map(Transaction transaction) {
        CustomerPatternState currentState = customerPatternState.value();
        
        // Fraud detection rules:
        // 1. Amount threshold check
        if (transaction.getAmount().compareTo(currentState.getMaxNormalAmount()) > 0) {
            return new FraudDetectionResult(transaction.getId(), "HIGH_AMOUNT", 0.8);
        }
        
        // 2. Frequency anomaly check
        if (currentState.getTransactionCount() > currentState.getMaxTransactionsPerHour()) {
            return new FraudDetectionResult(transaction.getId(), "FREQUENCY_ANOMALY", 0.7);
        }
        
        // 3. Location anomaly check
        if (!currentState.getNormalLocations().contains(transaction.getTransactionLocation())) {
            return new FraudDetectionResult(transaction.getId(), "LOCATION_ANOMALY", 0.6);
        }
        
        // 4. Time pattern anomaly
        if (!currentState.getNormalTransactionHours().contains(transaction.getCreatedAt().getHours())) {
            return new FraudDetectionResult(transaction.getId(), "TIME_ANOMALY", 0.5);
        }
        
        return new FraudDetectionResult(transaction.getId(), "NORMAL", 0.1);
    }
}
```

##### **2. Transaction Metrics Job:**
```java
// Logic: Tính toán metrics theo customer trong window 5 phút
public class TransactionMetricsProcessor extends ProcessWindowFunction<Transaction, TransactionMetrics, String, TimeWindow> {
    
    public void process(String customerId, Context context, Iterable<Transaction> transactions, Collector<TransactionMetrics> out) {
        
        // Tính toán metrics:
        BigDecimal totalAmount = BigDecimal.ZERO;
        BigDecimal totalFees = BigDecimal.ZERO;
        int transactionCount = 0;
        Map<String, Integer> typeDistribution = new HashMap<>();
        
        for (Transaction t : transactions) {
            totalAmount = totalAmount.add(t.getAmount());
            totalFees = totalFees.add(t.getFeeAmount());
            transactionCount++;
            
            // Type distribution
            String type = t.getTransactionType().toString();
            typeDistribution.put(type, typeDistribution.getOrDefault(type, 0) + 1);
        }
        
        // Tính average và trends
        BigDecimal avgAmount = totalAmount.divide(BigDecimal.valueOf(transactionCount), 2, RoundingMode.HALF_UP);
        
        TransactionMetrics metrics = new TransactionMetrics(
            customerId, context.window().getStart(), context.window().getEnd(),
            transactionCount, totalAmount, avgAmount, totalFees, typeDistribution
        );
        
        out.collect(metrics);
    }
}
```

##### **3. Customer Behavior Job:**
```java
// Logic: Phân tích hành vi khách hàng trong window 15 phút
public class CustomerBehaviorProcessor extends ProcessWindowFunction<Transaction, CustomerBehaviorMetrics, String, TimeWindow> {
    
    public void process(String customerId, Context context, Iterable<Transaction> transactions, Collector<CustomerBehaviorMetrics> out) {
        
        // Behavioral analysis:
        Map<String, Integer> channelUsage = new HashMap<>();
        Map<String, BigDecimal> spendingByCategory = new HashMap<>();
        List<String> locations = new ArrayList<>();
        List<String> devices = new ArrayList<>();
        
        for (Transaction t : transactions) {
            // Channel analysis
            String channel = t.getNetwork(); // mobile, web, atm
            channelUsage.put(channel, channelUsage.getOrDefault(channel, 0) + 1);
            
            // Category spending
            String category = getMerchantCategory(t.getMerchantId());
            spendingByCategory.merge(category, t.getAmount(), BigDecimal::add);
            
            // Location tracking
            if (t.getTransactionLocation() != null) {
                locations.add(t.getTransactionLocation().get("city"));
            }
            
            // Device tracking
            if (t.getDeviceFingerprint() != null) {
                devices.add(t.getDeviceFingerprint());
            }
        }
        
        // Calculate behavioral scores
        double locationConsistency = calculateLocationConsistency(locations);
        double deviceConsistency = calculateDeviceConsistency(devices);
        double spendingPattern = calculateSpendingPattern(spendingByCategory);
        
        CustomerBehaviorMetrics behavior = new CustomerBehaviorMetrics(
            customerId, context.window().getStart(), context.window().getEnd(),
            channelUsage, spendingByCategory, locationConsistency, 
            deviceConsistency, spendingPattern
        );
        
        out.collect(behavior);
    }
}
```

##### **4. Risk Analytics Job:**
```java
// Logic: Đánh giá rủi ro theo customer trong window 10 phút
public class RiskAnalyticsProcessor extends ProcessWindowFunction<Transaction, RiskAnalytics, String, TimeWindow> {
    
    public void process(String customerId, Context context, Iterable<Transaction> transactions, Collector<RiskAnalytics> out) {
        
        // Risk assessment factors:
        BigDecimal totalExposure = BigDecimal.ZERO;
        int highRiskTransactions = 0;
        List<String> complianceViolations = new ArrayList<>();
        Map<String, Integer> riskFactors = new HashMap<>();
        
        for (Transaction t : transactions) {
            // Exposure calculation
            totalExposure = totalExposure.add(t.getAmount());
            
            // High risk transactions
            if (t.getRiskLevel() == RiskLevel.HIGH || t.getRiskLevel() == RiskLevel.CRITICAL) {
                highRiskTransactions++;
            }
            
            // Compliance violations
            if (!t.getComplianceFlags().isEmpty()) {
                complianceViolations.addAll(t.getComplianceFlags());
            }
            
            // Risk factor analysis
            analyzeRiskFactors(t, riskFactors);
        }
        
        // Calculate risk score
        double riskScore = calculateRiskScore(
            totalExposure, highRiskTransactions, 
            complianceViolations.size(), riskFactors
        );
        
        RiskAnalytics risk = new RiskAnalytics(
            customerId, context.window().getStart(), context.window().getEnd(),
            riskScore, totalExposure, highRiskTransactions, 
            complianceViolations, riskFactors
        );
        
        out.collect(risk);
    }
}
```

##### **5. Dashboard Metrics Job:**
```java
// Logic: Metrics tổng hợp cho dashboard trong window 1 phút
public class DashboardMetricsProcessor extends ProcessWindowFunction<Transaction, DashboardMetrics, String, TimeWindow> {
    
    public void process(String transactionType, Context context, Iterable<Transaction> transactions, Collector<DashboardMetrics> out) {
        
        // Real-time dashboard metrics:
        BigDecimal totalVolume = BigDecimal.ZERO;
        int transactionCount = 0;
        BigDecimal totalFees = BigDecimal.ZERO;
        Map<String, Integer> statusDistribution = new HashMap<>();
        Map<String, Integer> riskDistribution = new HashMap<>();
        
        for (Transaction t : transactions) {
            totalVolume = totalVolume.add(t.getAmount());
            transactionCount++;
            totalFees = totalFees.add(t.getFeeAmount());
            
            // Status distribution
            String status = t.getStatus().toString();
            statusDistribution.put(status, statusDistribution.getOrDefault(status, 0) + 1);
            
            // Risk distribution
            String risk = t.getRiskLevel().toString();
            riskDistribution.put(risk, riskDistribution.getOrDefault(risk, 0) + 1);
        }
        
        // Calculate KPIs
        BigDecimal avgTransactionValue = totalVolume.divide(BigDecimal.valueOf(transactionCount), 2, RoundingMode.HALF_UP);
        double successRate = (double) statusDistribution.getOrDefault("COMPLETED", 0) / transactionCount;
        
        DashboardMetrics metrics = new DashboardMetrics(
            transactionType, context.window().getStart(), context.window().getEnd(),
            transactionCount, totalVolume, avgTransactionValue, totalFees,
            successRate, statusDistribution, riskDistribution
        );
        
        out.collect(metrics);
    }
}
```

##### **6. Anomaly Detection Job:**
```java
// Logic: Phát hiện bất thường real-time
public class AnomalyDetectionFunction extends RichMapFunction<Transaction, AnomalyAlert> {
    
    private ValueState<CustomerPatternState> customerPatternState;
    
    public AnomalyAlert map(Transaction transaction) {
        CustomerPatternState currentState = customerPatternState.value();
        
        // Anomaly detection algorithms:
        
        // 1. Statistical outlier detection (Z-score)
        double zScore = calculateZScore(transaction.getAmount(), currentState.getAmountMean(), currentState.getAmountStdDev());
        if (Math.abs(zScore) > 3.0) {
            return new AnomalyAlert(transaction.getId(), "STATISTICAL_OUTLIER", zScore, "Amount deviation");
        }
        
        // 2. Pattern deviation detection
        if (isPatternDeviation(transaction, currentState)) {
            return new AnomalyAlert(transaction.getId(), "PATTERN_DEVIATION", 0.8, "Behavioral change");
        }
        
        // 3. Velocity anomaly detection
        if (isVelocityAnomaly(transaction, currentState)) {
            return new AnomalyAlert(transaction.getId(), "VELOCITY_ANOMALY", 0.7, "Transaction frequency change");
        }
        
        return null; // No anomaly detected
    }
}
```

##### **7. Geographic Analysis Job:**
```java
// Logic: Phân tích theo địa lý trong window 5 phút
public class GeographicMetricsProcessor extends ProcessWindowFunction<Transaction, GeographicMetrics, String, TimeWindow> {
    
    public void process(String country, Context context, Iterable<Transaction> transactions, Collector<GeographicMetrics> out) {
        
        // Geographic analysis:
        Map<String, Integer> cityDistribution = new HashMap<>();
        Map<String, BigDecimal> spendingByCity = new HashMap<>();
        Map<String, Integer> merchantDistribution = new HashMap<>();
        List<String> currencies = new ArrayList<>();
        
        for (Transaction t : transactions) {
            if (t.getTransactionLocation() != null) {
                String city = t.getTransactionLocation().get("city");
                cityDistribution.put(city, cityDistribution.getOrDefault(city, 0) + 1);
                
                // Spending by city
                spendingByCity.merge(city, t.getAmount(), BigDecimal::add);
                
                // Merchant distribution
                if (t.getMerchantId() != null) {
                    String merchantType = getMerchantType(t.getMerchantId());
                    merchantDistribution.put(merchantType, merchantDistribution.getOrDefault(merchantType, 0) + 1);
                }
                
                // Currency tracking
                currencies.add(t.getCurrency());
            }
        }
        
        GeographicMetrics geo = new GeographicMetrics(
            country, context.window().getStart(), context.window().getEnd(),
            cityDistribution, spendingByCity, merchantDistribution, currencies
        );
        
        out.collect(geo);
    }
}
```

##### **8. Merchant Analysis Job:**
```java
// Logic: Phân tích theo merchant trong window 10 phút
public class MerchantMetricsProcessor extends ProcessWindowFunction<Transaction, MerchantMetrics, String, TimeWindow> {
    
    public void process(String merchantId, Context context, Iterable<Transaction> transactions, Collector<MerchantMetrics> out) {
        
        // Merchant performance analysis:
        BigDecimal totalRevenue = BigDecimal.ZERO;
        int transactionCount = 0;
        Map<String, Integer> customerDistribution = new HashMap<>();
        Map<String, BigDecimal> amountDistribution = new HashMap<>();
        List<String> locations = new ArrayList<>();
        
        for (Transaction t : transactions) {
            totalRevenue = totalRevenue.add(t.getAmount());
            transactionCount++;
            
            // Customer distribution
            customerDistribution.put(t.getCustomerId(), customerDistribution.getOrDefault(t.getCustomerId(), 0) + 1);
            
            // Amount distribution by range
            String amountRange = getAmountRange(t.getAmount());
            amountDistribution.merge(amountRange, t.getAmount(), BigDecimal::add);
            
            // Location tracking
            if (t.getTransactionLocation() != null) {
                locations.add(t.getTransactionLocation().get("city"));
            }
        }
        
        // Calculate merchant KPIs
        BigDecimal avgTransactionValue = totalRevenue.divide(BigDecimal.valueOf(transactionCount), 2, RoundingMode.HALF_UP);
        int uniqueCustomers = customerDistribution.size();
        double customerRetention = calculateCustomerRetention(customerDistribution);
        
        MerchantMetrics merchant = new MerchantMetrics(
            merchantId, context.window().getStart(), context.window().getEnd(),
            totalRevenue, transactionCount, avgTransactionValue, uniqueCustomers,
            customerRetention, customerDistribution, amountDistribution, locations
        );
        
        out.collect(merchant);
    }
}
```

##### **9. Time Pattern Analysis Job:**
```java
// Logic: Phân tích pattern theo giờ trong window 1 giờ
public class TimePatternProcessor extends ProcessWindowFunction<Transaction, TimePatternMetrics, Integer, TimeWindow> {
    
    public void process(Integer hour, Context context, Iterable<Transaction> transactions, Collector<TimePatternMetrics> out) {
        
        // Time-based pattern analysis:
        Map<String, Integer> transactionTypeDistribution = new HashMap<>();
        Map<String, BigDecimal> spendingByType = new HashMap<>();
        Map<String, Integer> customerActivity = new HashMap<>();
        List<String> peakPeriods = new ArrayList<>();
        
        for (Transaction t : transactions) {
            // Transaction type distribution by hour
            String type = t.getTransactionType().toString();
            transactionTypeDistribution.put(type, transactionTypeDistribution.getOrDefault(type, 0) + 1);
            
            // Spending patterns by type
            spendingByType.merge(type, t.getAmount(), BigDecimal::add);
            
            // Customer activity patterns
            customerActivity.put(t.getCustomerId(), customerActivity.getOrDefault(t.getCustomerId(), 0) + 1);
        }
        
        // Identify peak periods
        if (hour >= 9 && hour <= 11) peakPeriods.add("MORNING_PEAK");
        if (hour >= 12 && hour <= 14) peakPeriods.add("LUNCH_PEAK");
        if (hour >= 17 && hour <= 19) peakPeriods.add("EVENING_PEAK");
        
        TimePatternMetrics timePattern = new TimePatternMetrics(
            hour, context.window().getStart(), context.window().getEnd(),
            transactionTypeDistribution, spendingByType, customerActivity, peakPeriods
        );
        
        out.collect(timePattern);
    }
}
```

##### **10. Compliance Monitoring Job:**
```java
// Logic: Giám sát compliance theo customer trong window 30 phút
public class ComplianceProcessor extends ProcessWindowFunction<Transaction, ComplianceMetrics, String, TimeWindow> {
    
    public void process(String customerId, Context context, Iterable<Transaction> transactions, Collector<ComplianceMetrics> out) {
        
        // Compliance monitoring:
        Map<String, Integer> violationTypes = new HashMap<>();
        List<String> flaggedTransactions = new ArrayList<>();
        BigDecimal totalViolationAmount = BigDecimal.ZERO;
        Map<String, Integer> riskLevelDistribution = new HashMap<>();
        
        for (Transaction t : transactions) {
            // Compliance violations
            for (String flag : t.getComplianceFlags()) {
                violationTypes.put(flag, violationTypes.getOrDefault(flag, 0) + 1);
                flaggedTransactions.add(t.getId());
                totalViolationAmount = totalViolationAmount.add(t.getAmount());
            }
            
            // Risk level distribution
            String riskLevel = t.getRiskLevel().toString();
            riskLevelDistribution.put(riskLevel, riskLevelDistribution.getOrDefault(riskLevel, 0) + 1);
        }
        
        // Calculate compliance score
        double complianceScore = calculateComplianceScore(violationTypes, flaggedTransactions.size());
        boolean requiresReview = complianceScore < 0.7;
        
        ComplianceMetrics compliance = new ComplianceMetrics(
            customerId, context.window().getStart(), context.window().getEnd(),
            complianceScore, requiresReview, violationTypes, flaggedTransactions,
            totalViolationAmount, riskLevelDistribution
        );
        
        out.collect(compliance);
    }
}
```

### **Job Configuration**

#### **Flink Settings:**
```yaml
# flink-conf.yaml
jobmanager.rpc.address: localhost
taskmanager.numberOfTaskSlots: 4
parallelism.default: 2
state.backend: rocksdb
checkpointing.interval: 10000ms
```

#### **Build và Run:**
```bash
# Build
cd flink-jobs
mvn clean package

# Run
flink run target/flink-jobs-1.0-SNAPSHOT.jar
```

## 📈 Grafana Integration và Data Visualization

### **Data Flow to Grafana**

#### **1. Flink Output Streams:**
```java
// Các streams được xử lý real-time
DataStream<DashboardMetrics> dashboardMetricsStream = 
    transactionStream.keyBy(Transaction::getTransactionType)
                    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                    .process(new DashboardMetricsProcessor());
```

#### **2. Kafka Sink (Output Topics):**
- **`fintech.analytics.fraud_detection`**: Kết quả phát hiện gian lận
- **`fintech.analytics.transaction_metrics`**: Metrics giao dịch
- **`fintech.analytics.customer_behavior`**: Phân tích hành vi khách hàng
- **`fintech.analytics.risk_analytics`**: Phân tích rủi ro
- **`fintech.analytics.dashboard_metrics`**: Metrics cho dashboard

#### **3. Grafana Data Sources:**
- **Kafka**: Real-time streaming data
- **PostgreSQL**: Historical data và reference data
- **Prometheus**: Metrics và monitoring data

### **Dashboard Components**

#### **Real-time Dashboards:**
1. **Transaction Monitor**: Số lượng giao dịch theo thời gian thực
2. **Fraud Detection**: Cảnh báo gian lận và risk scores
3. **Customer Analytics**: Hành vi và metrics khách hàng
4. **Risk Management**: Risk scores và compliance monitoring
5. **Geographic Analysis**: Phân tích theo địa lý
6. **Merchant Performance**: Hiệu suất thương nhân

#### **Key Metrics Displayed:**
- **Transaction Volume**: Số lượng giao dịch theo thời gian
- **Risk Scores**: Risk levels và trends
- **Fraud Alerts**: Số lượng và severity của cảnh báo
- **Customer Behavior**: Spending patterns và engagement
- **Geographic Distribution**: Phân bố giao dịch theo vùng
- **Compliance Status**: Compliance flags và violations

### **Real-time Updates**

#### **Update Frequency:**
- **Dashboard Metrics**: Cập nhật mỗi phút
- **Transaction Metrics**: Cập nhật mỗi 5 phút
- **Customer Behavior**: Cập nhật mỗi 15 phút
- **Risk Analytics**: Cập nhật mỗi 10 phút

#### **Data Freshness:**
- **End-to-end latency**: < 1 giây
- **Data consistency**: Đảm bảo bởi Flink checkpointing
- **Real-time processing**: Không có batch delays

## 🚀 Quick Start

### **1. Start Infrastructure:**
```bash
cd docker
docker-compose up -d
```

### **2. Setup Python App:**
```bash
cd app-python
python main.py run-all --rate 10 --num-customers 100
```

### **3. Run Flink Job:**
```bash
cd flink-jobs
mvn clean package
flink run target/flink-jobs-1.0-SNAPSHOT.jar
```

### **4. Access Dashboards:**
- **Flink UI**: http://localhost:8081
- **Kafka UI**: http://localhost:8080
- **Grafana**: http://localhost:3000 (admin/admin)

## 🔧 Architecture Benefits

### **1. Real-time Processing:**
- **Sub-second latency** cho fraud detection
- **Continuous processing** không có batch delays
- **Real-time analytics** cho business decisions

### **2. Scalability:**
- **Horizontal scaling** với Flink parallelism
- **Kafka partitioning** cho throughput cao
- **Database sharding** support

### **3. Reliability:**
- **Outbox pattern** đảm bảo event delivery
- **Flink checkpointing** cho fault tolerance
- **Debezium CDC** cho data consistency

### **4. Flexibility:**
- **Multiple data sources** (tables + outbox)
- **Configurable processing** với time windows
- **Extensible analytics** pipelines

## 📚 Technologies Used

- **Data Generation**: Python, Faker, Pydantic
- **Database**: PostgreSQL với logical replication
- **CDC**: Debezium với outbox pattern
- **Streaming**: Apache Kafka
- **Processing**: Apache Flink 1.18.1
- **Visualization**: Grafana
- **Infrastructure**: Docker, Docker Compose

---

**Tác giả**: Flink Fintech POC Team  
**Phiên bản**: 1.0  
**Cập nhật**: 2024
