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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Real-time anomaly detection function
 */
public class AnomalyDetectionFunction extends RichMapFunction<Transaction, AnomalyAlert> {
    
    private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectionFunction.class);
    
    // State to track customer behavior patterns
    private ValueState<CustomerPatternState> customerPatternState;
    
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<CustomerPatternState> descriptor = 
            new ValueStateDescriptor<>("customer-pattern", CustomerPatternState.class);
        customerPatternState = getRuntimeContext().getState(descriptor);
    }
    
    @Override
    public AnomalyAlert map(Transaction transaction) throws Exception {
        try {
            // Get current customer pattern state
            CustomerPatternState currentState = customerPatternState.value();
            if (currentState == null) {
                currentState = new CustomerPatternState(transaction.getCustomerId());
            }
            
            // Update state with current transaction
            currentState.updateWithTransaction(transaction);
            customerPatternState.update(currentState);
            
            // Perform anomaly detection
            AnomalyAlert alert = detectAnomalies(transaction, currentState);
            
            if (alert != null) {
                LOG.info("Anomaly detected for customer {}: {}", 
                        transaction.getCustomerId(), alert.getAlertType());
            }
            
            return alert;
            
        } catch (Exception e) {
            LOG.error("Error in anomaly detection for transaction: " + transaction.getId(), e);
            // Return null to indicate no anomaly
            return null;
        }
    }
    
    private AnomalyAlert detectAnomalies(Transaction transaction, CustomerPatternState state) {
        List<String> triggeredRules = new ArrayList<>();
        double confidenceScore = 0.0;
        String alertType = null;
        String severity = "low";
        String description = "";
        
        // Rule 1: Amount anomaly
        if (isAmountAnomaly(transaction.getAmount(), state)) {
            triggeredRules.add("amount_anomaly");
            confidenceScore = Math.max(confidenceScore, 0.7);
            alertType = "amount_anomaly";
            severity = "medium";
            description = "Unusual transaction amount detected";
        }
        
        // Rule 2: Frequency anomaly
        if (isFrequencyAnomaly(state)) {
            triggeredRules.add("frequency_anomaly");
            confidenceScore = Math.max(confidenceScore, 0.8);
            alertType = "frequency_anomaly";
            severity = "high";
            description = "Unusual transaction frequency detected";
        }
        
        // Rule 3: Location anomaly
        if (isLocationAnomaly(transaction, state)) {
            triggeredRules.add("location_anomaly");
            confidenceScore = Math.max(confidenceScore, 0.6);
            alertType = "location_anomaly";
            severity = "medium";
            description = "Unusual transaction location detected";
        }
        
        // Rule 4: Time anomaly
        if (isTimeAnomaly(transaction, state)) {
            triggeredRules.add("time_anomaly");
            confidenceScore = Math.max(confidenceScore, 0.5);
            alertType = "time_anomaly";
            severity = "low";
            description = "Unusual transaction time detected";
        }
        
        // Rule 5: Device anomaly
        if (isDeviceAnomaly(transaction, state)) {
            triggeredRules.add("device_anomaly");
            confidenceScore = Math.max(confidenceScore, 0.7);
            alertType = "device_anomaly";
            severity = "medium";
            description = "Unusual device detected";
        }
        
        // Only create alert if anomalies are detected
        if (alertType != null && confidenceScore > 0.5) {
            AnomalyAlert alert = new AnomalyAlert(
                UUID.randomUUID().toString(),
                transaction.getCustomerId(),
                alertType,
                severity,
                description
            );
            
            alert.setConfidenceScore(confidenceScore);
            alert.setTransactionAmount(transaction.getAmount());
            alert.setTransactionType(transaction.getTransactionType());
            alert.setTriggeredRules(triggeredRules);
            alert.setDetectedAt(new Date());
            
            // Set anomaly details
            Map<String, Object> anomalyDetails = new HashMap<>();
            anomalyDetails.put("customer_id", transaction.getCustomerId());
            anomalyDetails.put("transaction_id", transaction.getId());
            anomalyDetails.put("amount", transaction.getAmount());
            anomalyDetails.put("location", transaction.getTransactionLocation());
            anomalyDetails.put("device", transaction.getDeviceFingerprint());
            anomalyDetails.put("risk_score", transaction.getRiskScore());
            alert.setAnomalyDetails(anomalyDetails);
            
            // Set recommendation
            if (confidenceScore > 0.8) {
                alert.setRecommendation("review_immediately");
            } else if (confidenceScore > 0.6) {
                alert.setRecommendation("review");
            } else {
                alert.setRecommendation("monitor");
            }
            
            return alert;
        }
        
        return null;
    }
    
    private boolean isAmountAnomaly(BigDecimal amount, CustomerPatternState state) {
        if (state.getAverageAmount().compareTo(BigDecimal.ZERO) == 0) {
            return false; // Not enough data
        }
        
        BigDecimal threshold = state.getAverageAmount().multiply(new BigDecimal("5"));
        return amount.compareTo(threshold) > 0;
    }
    
    private boolean isFrequencyAnomaly(CustomerPatternState state) {
        long timeWindowMs = 10 * 60 * 1000; // 10 minutes
        long currentTime = System.currentTimeMillis();
        long recentTransactions = state.getRecentTransactions().stream()
            .filter(timestamp -> (currentTime - timestamp) < timeWindowMs)
            .count();
        
        return recentTransactions > 20; // More than 20 transactions in 10 minutes
    }
    
    private boolean isLocationAnomaly(Transaction transaction, CustomerPatternState state) {
        if (transaction.getTransactionLocation() == null || state.getCommonLocations().isEmpty()) {
            return false;
        }
        
        String currentCountry = (String) transaction.getTransactionLocation().get("country");
        return !state.getCommonLocations().contains(currentCountry);
    }
    
    private boolean isTimeAnomaly(Transaction transaction, CustomerPatternState state) {
        // Check if transaction is outside normal business hours (simplified)
        // In production, you'd use more sophisticated time analysis
        return false;
    }
    
    private boolean isDeviceAnomaly(Transaction transaction, CustomerPatternState state) {
        if (transaction.getDeviceFingerprint() == null || state.getCommonDevices().isEmpty()) {
            return false;
        }
        
        return !state.getCommonDevices().contains(transaction.getDeviceFingerprint());
    }
    
    /**
     * Internal state class to track customer patterns
     */
    private static class CustomerPatternState {
        private String customerId;
        private List<Long> recentTransactions;
        private BigDecimal averageAmount;
        private List<String> commonLocations;
        private List<String> commonDevices;
        private Date lastUpdated;
        
        public CustomerPatternState(String customerId) {
            this.customerId = customerId;
            this.recentTransactions = new ArrayList<>();
            this.averageAmount = BigDecimal.ZERO;
            this.commonLocations = new ArrayList<>();
            this.commonDevices = new ArrayList<>();
            this.lastUpdated = new Date();
        }
        
        public void updateWithTransaction(Transaction transaction) {
            // Add current transaction timestamp
            recentTransactions.add(System.currentTimeMillis());
            
            // Keep only last 200 transactions
            if (recentTransactions.size() > 200) {
                recentTransactions.remove(0);
            }
            
            // Update average amount
            if (averageAmount.compareTo(BigDecimal.ZERO) == 0) {
                averageAmount = transaction.getAmount();
            } else {
                averageAmount = averageAmount.add(transaction.getAmount()).divide(new BigDecimal("2"));
            }
            
            // Update common locations
            if (transaction.getTransactionLocation() != null) {
                String country = (String) transaction.getTransactionLocation().get("country");
                if (country != null && !commonLocations.contains(country)) {
                    commonLocations.add(country);
                }
            }
            
            // Update common devices
            if (transaction.getDeviceFingerprint() != null && 
                !commonDevices.contains(transaction.getDeviceFingerprint())) {
                commonDevices.add(transaction.getDeviceFingerprint());
            }
            
            lastUpdated = new Date();
        }
        
        // Getters
        public String getCustomerId() { return customerId; }
        public List<Long> getRecentTransactions() { return recentTransactions; }
        public BigDecimal getAverageAmount() { return averageAmount; }
        public List<String> getCommonLocations() { return commonLocations; }
        public List<String> getCommonDevices() { return commonDevices; }
        public Date getLastUpdated() { return lastUpdated; }
    }
}
