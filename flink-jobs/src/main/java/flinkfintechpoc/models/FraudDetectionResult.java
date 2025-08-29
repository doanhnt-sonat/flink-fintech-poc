package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

/**
 * Result of fraud detection analysis
 */
public class FraudDetectionResult {
    private String id;
    private String customerId;
    private String transactionId;
    private boolean isFraudulent;
    private double fraudScore;
    private String fraudType;
    private List<String> triggeredRules;
    private String recommendation;
    private BigDecimal transactionAmount;
    private String transactionType;
    private Date detectedAt;
    private String riskLevel;
    
    // Constructors
    public FraudDetectionResult() {}
    
    public FraudDetectionResult(String id, String customerId, String transactionId, boolean isFraudulent, double fraudScore) {
        this.id = id;
        this.customerId = customerId;
        this.transactionId = transactionId;
        this.isFraudulent = isFraudulent;
        this.fraudScore = fraudScore;
        this.detectedAt = new Date();
    }
    
    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    
    public boolean isFraudulent() { return isFraudulent; }
    public void setFraudulent(boolean fraudulent) { isFraudulent = fraudulent; }
    
    public double getFraudScore() { return fraudScore; }
    public void setFraudScore(double fraudScore) { this.fraudScore = fraudScore; }
    
    public String getFraudType() { return fraudType; }
    public void setFraudType(String fraudType) { this.fraudType = fraudType; }
    
    public List<String> getTriggeredRules() { return triggeredRules; }
    public void setTriggeredRules(List<String> triggeredRules) { this.triggeredRules = triggeredRules; }
    
    public String getRecommendation() { return recommendation; }
    public void setRecommendation(String recommendation) { this.recommendation = recommendation; }
    
    public BigDecimal getTransactionAmount() { return transactionAmount; }
    public void setTransactionAmount(BigDecimal transactionAmount) { this.transactionAmount = transactionAmount; }
    
    public String getTransactionType() { return transactionType; }
    public void setTransactionType(String transactionType) { this.transactionType = transactionType; }
    
    public Date getDetectedAt() { return detectedAt; }
    public void setDetectedAt(Date detectedAt) { this.detectedAt = detectedAt; }
    
    public String getRiskLevel() { return riskLevel; }
    public void setRiskLevel(String riskLevel) { this.riskLevel = riskLevel; }
    
    @Override
    public String toString() {
        return "FraudDetectionResult{" +
                "id='" + id + '\'' +
                ", customerId='" + customerId + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", isFraudulent=" + isFraudulent +
                ", fraudScore=" + fraudScore +
                ", fraudType='" + fraudType + '\'' +
                ", detectedAt=" + detectedAt +
                '}';
    }
}
