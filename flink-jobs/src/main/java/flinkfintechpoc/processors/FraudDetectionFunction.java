package flinkfintechpoc.processors;

import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.AnomalyAlert;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Fraud Detection Function for real-time transaction monitoring
 * Detects suspicious patterns and generates alerts based on risk factors
 */
public class FraudDetectionFunction extends RichMapFunction<Transaction, AnomalyAlert> {
    
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionFunction.class);
    
    // Risk thresholds
    private static final double HIGH_RISK_THRESHOLD = 70.0;
    private static final double CRITICAL_RISK_THRESHOLD = 90.0;
    private static final BigDecimal SUSPICIOUS_AMOUNT_THRESHOLD = new BigDecimal("10000.00");
    private static final int HIGH_FREQUENCY_THRESHOLD = 10; // transactions per hour
    
    // State for tracking customer behavior
    private ValueState<CustomerTransactionState> customerState;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<CustomerTransactionState> descriptor = 
            new ValueStateDescriptor<>("customer-transaction-state", CustomerTransactionState.class);
        customerState = getRuntimeContext().getState(descriptor);
    }
    
    @Override
    public AnomalyAlert map(Transaction transaction) throws Exception {
        CustomerTransactionState state = customerState.value();
        if (state == null) {
            state = new CustomerTransactionState();
        }
        
        // Update state
        state.updateTransaction(transaction);
        customerState.update(state);
        
        // Analyze for fraud patterns
        FraudDetectionResult result = analyzeTransaction(transaction, state);
        
        if (result.isSuspicious()) {
            LOG.warn("Suspicious transaction detected - Transaction ID: {}, Customer ID: {}, Risk Score: {}, Alert Types: {}", 
                    transaction.getId(),
                    transaction.getCustomerId(),
                    result.getRiskScore(),
                    result.getAlertTypes());
            
            return createAnomalyAlert(transaction, result);
        }
        
        // Return null if no alert needed
        return null;
    }
    
    private FraudDetectionResult analyzeTransaction(Transaction transaction, CustomerTransactionState state) {
        FraudDetectionResult result = new FraudDetectionResult();
        double riskScore = transaction.getRiskScore();
        
        // 1. High risk score from upstream
        if (riskScore >= CRITICAL_RISK_THRESHOLD) {
            result.addAlert("CRITICAL_RISK_SCORE", "Transaction has critical risk score: " + riskScore);
            result.setRiskScore(Math.max(result.getRiskScore(), riskScore));
        } else if (riskScore >= HIGH_RISK_THRESHOLD) {
            result.addAlert("HIGH_RISK_SCORE", "Transaction has high risk score: " + riskScore);
            result.setRiskScore(Math.max(result.getRiskScore(), riskScore));
        }
        
        // 2. Suspicious amount
        if (transaction.getAmount().compareTo(SUSPICIOUS_AMOUNT_THRESHOLD) > 0) {
            result.addAlert("SUSPICIOUS_AMOUNT", "Transaction amount exceeds threshold: " + transaction.getAmount());
            result.setRiskScore(Math.max(result.getRiskScore(), 80.0));
        }
        
        // 3. High frequency transactions
        if (state.getRecentTransactionCount() > HIGH_FREQUENCY_THRESHOLD) {
            result.addAlert("HIGH_FREQUENCY", "Customer has " + state.getRecentTransactionCount() + " transactions in last hour");
            result.setRiskScore(Math.max(result.getRiskScore(), 75.0));
        }
        
        // 4. Geographic anomaly
        if (state.hasGeographicAnomaly(transaction)) {
            result.addAlert("GEOGRAPHIC_ANOMALY", "Transaction location differs from customer's usual pattern");
            result.setRiskScore(Math.max(result.getRiskScore(), 70.0));
        }
        
        // 5. Device anomaly
        if (state.hasDeviceAnomaly(transaction)) {
            result.addAlert("DEVICE_ANOMALY", "Transaction from unusual device or IP");
            result.setRiskScore(Math.max(result.getRiskScore(), 65.0));
        }
        
        // 6. Time-based anomaly
        if (state.hasTimeAnomaly(transaction)) {
            result.addAlert("TIME_ANOMALY", "Transaction at unusual time for customer");
            result.setRiskScore(Math.max(result.getRiskScore(), 60.0));
        }
        
        // 7. Compliance flags
        if (transaction.getComplianceFlags() != null && !transaction.getComplianceFlags().isEmpty()) {
            for (String flag : transaction.getComplianceFlags()) {
                if (flag.equals("HIGH_VALUE")) {
                    result.addAlert("HIGH_VALUE_FLAG", "Transaction flagged as high value");
                    result.setRiskScore(Math.max(result.getRiskScore(), 85.0));
                } else if (flag.equals("HIGH_RISK")) {
                    result.addAlert("HIGH_RISK_FLAG", "Transaction flagged as high risk");
                    result.setRiskScore(Math.max(result.getRiskScore(), 90.0));
                }
            }
        }
        
        return result;
    }
    
    private AnomalyAlert createAnomalyAlert(Transaction transaction, FraudDetectionResult result) {
        AnomalyAlert alert = new AnomalyAlert();
        alert.setId("ALERT-" + System.currentTimeMillis());
        alert.setCustomerId(transaction.getCustomerId());
        alert.setAlertType("FRAUD_DETECTION");
        alert.setSeverity(result.getRiskScore() >= CRITICAL_RISK_THRESHOLD ? "CRITICAL" : "HIGH");
        alert.setDescription("Fraud detection alert: " + String.join(", ", result.getAlertTypes()));
        alert.setConfidenceScore(Math.min(result.getRiskScore() / 100.0, 0.95));
        alert.setTriggeredRules(result.getAlertTypes());
        alert.setTransactionAmount(transaction.getAmount());
        alert.setTransactionType(transaction.getTransactionType());
        alert.setDetectedAt(new Date());
        
        return alert;
    }
    
    /**
     * Customer transaction state for fraud detection
     */
    public static class CustomerTransactionState {
        private String customerId;
        private Map<String, Integer> locationCounts = new HashMap<>();
        private Map<String, Integer> deviceCounts = new HashMap<>();
        private int recentTransactionCount = 0;
        private long lastTransactionTime = 0;
        private BigDecimal totalAmount = BigDecimal.ZERO;
        
        public void updateTransaction(Transaction transaction) {
            this.customerId = transaction.getCustomerId();
            this.recentTransactionCount++;
            this.lastTransactionTime = System.currentTimeMillis();
            this.totalAmount = this.totalAmount.add(transaction.getAmount());
            
            // Update location counts
            if (transaction.getTransactionLocation() != null) {
                String country = (String) transaction.getTransactionLocation().get("country");
                if (country != null) {
                    locationCounts.put(country, locationCounts.getOrDefault(country, 0) + 1);
                }
            }
            
            // Update device counts
            if (transaction.getDeviceFingerprint() != null) {
                deviceCounts.put(transaction.getDeviceFingerprint(), 
                               deviceCounts.getOrDefault(transaction.getDeviceFingerprint(), 0) + 1);
            }
        }
        
        public int getRecentTransactionCount() {
            // Reset count if more than 1 hour has passed
            if (System.currentTimeMillis() - lastTransactionTime > 3600000) {
                recentTransactionCount = 1;
            }
            return recentTransactionCount;
        }
        
        public boolean hasGeographicAnomaly(Transaction transaction) {
            if (transaction.getTransactionLocation() == null) return false;
            
            String country = (String) transaction.getTransactionLocation().get("country");
            if (country == null) return false;
            
            // If this is a new country and customer has transactions in other countries
            if (!locationCounts.containsKey(country) && locationCounts.size() > 0) {
                return true;
            }
            
            return false;
        }
        
        public boolean hasDeviceAnomaly(Transaction transaction) {
            if (transaction.getDeviceFingerprint() == null) return false;
            
            // If this is a new device and customer has used other devices
            if (!deviceCounts.containsKey(transaction.getDeviceFingerprint()) && deviceCounts.size() > 0) {
                return true;
            }
            
            return false;
        }
        
        public boolean hasTimeAnomaly(Transaction transaction) {
            // Simple time anomaly: transactions between 2 AM and 6 AM
            Date transactionTime = transaction.getCreatedAt();
            if (transactionTime != null) {
                int hour = transactionTime.getHours();
                if (hour >= 2 && hour <= 6) {
                    return true;
                }
            }
            return false;
        }
    }
    
    /**
     * Fraud detection result
     */
    public static class FraudDetectionResult {
        private double riskScore = 0.0;
        private java.util.List<String> alertTypes = new java.util.ArrayList<>();
        
        public void addAlert(String type, String description) {
            alertTypes.add(type + ": " + description);
        }
        
        public boolean isSuspicious() {
            return riskScore >= HIGH_RISK_THRESHOLD || !alertTypes.isEmpty();
        }
        
        public double getRiskScore() { return riskScore; }
        public void setRiskScore(double riskScore) { this.riskScore = riskScore; }
        public java.util.List<String> getAlertTypes() { return alertTypes; }
    }
}
