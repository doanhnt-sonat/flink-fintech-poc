package flinkfintechpoc.processors;

import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.FraudDetectionResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * Real-time fraud detection function for transactions
 */
public class FraudDetectionFunction extends RichMapFunction<Transaction, FraudDetectionResult> {
    
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionFunction.class);
    
    // State to track customer behavior
    private ValueState<CustomerBehaviorState> customerBehaviorState;
    
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<CustomerBehaviorState> descriptor = 
            new ValueStateDescriptor<>("customer-behavior", CustomerBehaviorState.class);
        customerBehaviorState = getRuntimeContext().getState(descriptor);
    }
    
    @Override
    public FraudDetectionResult map(Transaction transaction) throws Exception {
        try {
            // Get current customer behavior state
            CustomerBehaviorState currentState = customerBehaviorState.value();
            if (currentState == null) {
                currentState = new CustomerBehaviorState(transaction.getCustomerId());
            }
            
            // Update state with current transaction
            currentState.updateWithTransaction(transaction);
            customerBehaviorState.update(currentState);
            
            // Perform fraud detection
            FraudDetectionResult result = detectFraud(transaction, currentState);
            
            LOG.info("Fraud detection completed for transaction {}: {}", 
                    transaction.getId(), result.isFraudulent() ? "FRAUDULENT" : "LEGITIMATE");
            
            return result;
            
        } catch (Exception e) {
            LOG.error("Error in fraud detection for transaction: " + transaction.getId(), e);
            // Return safe default
            return new FraudDetectionResult(
                UUID.randomUUID().toString(),
                transaction.getCustomerId(),
                transaction.getId(),
                false,
                0.0
            );
        }
    }
    
    private FraudDetectionResult detectFraud(Transaction transaction, CustomerBehaviorState state) {
        List<String> triggeredRules = new ArrayList<>();
        double fraudScore = 0.0;
        String fraudType = "none";
        String recommendation = "allow";
        
        // Rule 1: Unusual transaction amount
        if (isUnusualAmount(transaction.getAmount(), state)) {
            fraudScore += 25.0;
            triggeredRules.add("unusual_amount");
            fraudType = "amount_anomaly";
        }
        
        // Rule 2: High frequency transactions
        if (isHighFrequency(state)) {
            fraudScore += 30.0;
            triggeredRules.add("high_frequency");
            fraudType = "frequency_anomaly";
        }
        
        // Rule 3: Geographic anomaly
        if (isGeographicAnomaly(transaction, state)) {
            fraudScore += 35.0;
            triggeredRules.add("geographic_anomaly");
            fraudType = "location_anomaly";
        }
        
        // Rule 4: Device fingerprint anomaly
        if (isDeviceAnomaly(transaction, state)) {
            fraudScore += 20.0;
            triggeredRules.add("device_anomaly");
            fraudType = "device_anomaly";
        }
        
        // Rule 5: Time-based anomaly
        if (isTimeAnomaly(transaction, state)) {
            fraudScore += 15.0;
            triggeredRules.add("time_anomaly");
            fraudType = "time_anomaly";
        }
        
        // Rule 6: Risk score threshold
        if (transaction.getRiskScore() > 80.0) {
            fraudScore += 40.0;
            triggeredRules.add("high_risk_score");
            fraudType = "high_risk";
        }
        
        // Determine if transaction is fraudulent
        boolean isFraudulent = fraudScore >= 50.0;
        
        // Set recommendation
        if (fraudScore >= 80.0) {
            recommendation = "block";
        } else if (fraudScore >= 50.0) {
            recommendation = "review";
        } else {
            recommendation = "allow";
        }
        
        // Determine risk level
        String riskLevel = "low";
        if (fraudScore >= 80.0) {
            riskLevel = "critical";
        } else if (fraudScore >= 60.0) {
            riskLevel = "high";
        } else if (fraudScore >= 40.0) {
            riskLevel = "medium";
        }
        
        return new FraudDetectionResult(
            UUID.randomUUID().toString(),
            transaction.getCustomerId(),
            transaction.getId(),
            isFraudulent,
            fraudScore
        );
    }
    
    private boolean isUnusualAmount(BigDecimal amount, CustomerBehaviorState state) {
        if (state.getAverageAmount().compareTo(BigDecimal.ZERO) == 0) {
            return false; // Not enough data
        }
        
        BigDecimal threshold = state.getAverageAmount().multiply(new BigDecimal("3"));
        return amount.compareTo(threshold) > 0;
    }
    
    private boolean isHighFrequency(CustomerBehaviorState state) {
        long timeWindowMs = 5 * 60 * 1000; // 5 minutes
        long currentTime = System.currentTimeMillis();
        long recentTransactions = state.getRecentTransactions().stream()
            .filter(timestamp -> (currentTime - timestamp) < timeWindowMs)
            .count();
        
        return recentTransactions > 10; // More than 10 transactions in 5 minutes
    }
    
    private boolean isGeographicAnomaly(Transaction transaction, CustomerBehaviorState state) {
        if (transaction.getTransactionLocation() == null || state.getCommonLocations().isEmpty()) {
            return false;
        }
        
        String currentCountry = (String) transaction.getTransactionLocation().get("country");
        return !state.getCommonLocations().contains(currentCountry);
    }
    
    private boolean isDeviceAnomaly(Transaction transaction, CustomerBehaviorState state) {
        if (transaction.getDeviceFingerprint() == null || state.getCommonDevices().isEmpty()) {
            return false;
        }
        
        return !state.getCommonDevices().contains(transaction.getDeviceFingerprint());
    }
    
    private boolean isTimeAnomaly(Transaction transaction, CustomerBehaviorState state) {
        // Check if transaction is outside normal business hours
        // This is a simplified check - in production you'd use more sophisticated logic
        return false;
    }
    
    /**
     * Internal state class to track customer behavior
     */
    private static class CustomerBehaviorState {
        private String customerId;
        private List<Long> recentTransactions;
        private BigDecimal averageAmount;
        private List<String> commonLocations;
        private List<String> commonDevices;
        private Date lastUpdated;
        
        public CustomerBehaviorState(String customerId) {
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
            
            // Keep only last 100 transactions
            if (recentTransactions.size() > 100) {
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
