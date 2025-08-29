package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Anomaly alert for unusual patterns
 */
public class AnomalyAlert {
    private String id;
    private String customerId;
    private String alertType;
    private String severity;
    private String description;
    private double confidenceScore;
    private BigDecimal transactionAmount;
    private String transactionType;
    private Map<String, Object> anomalyDetails;
    private List<String> triggeredRules;
    private Date detectedAt;
    private String status;
    private String recommendation;
    
    // Constructors
    public AnomalyAlert() {}
    
    public AnomalyAlert(String id, String customerId, String alertType, String severity, String description) {
        this.id = id;
        this.customerId = customerId;
        this.alertType = alertType;
        this.severity = severity;
        this.description = description;
        this.detectedAt = new Date();
        this.status = "new";
    }
    
    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public String getAlertType() { return alertType; }
    public void setAlertType(String alertType) { this.alertType = alertType; }
    
    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }
    
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    
    public double getConfidenceScore() { return confidenceScore; }
    public void setConfidenceScore(double confidenceScore) { this.confidenceScore = confidenceScore; }
    
    public BigDecimal getTransactionAmount() { return transactionAmount; }
    public void setTransactionAmount(BigDecimal transactionAmount) { this.transactionAmount = transactionAmount; }
    
    public String getTransactionType() { return transactionType; }
    public void setTransactionType(String transactionType) { this.transactionType = transactionType; }
    
    public Map<String, Object> getAnomalyDetails() { return anomalyDetails; }
    public void setAnomalyDetails(Map<String, Object> anomalyDetails) { this.anomalyDetails = anomalyDetails; }
    
    public List<String> getTriggeredRules() { return triggeredRules; }
    public void setTriggeredRules(List<String> triggeredRules) { this.triggeredRules = triggeredRules; }
    
    public Date getDetectedAt() { return detectedAt; }
    public void setDetectedAt(Date detectedAt) { this.detectedAt = detectedAt; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    public String getRecommendation() { return recommendation; }
    public void setRecommendation(String recommendation) { this.recommendation = recommendation; }
    
    @Override
    public String toString() {
        return "AnomalyAlert{" +
                "id='" + id + '\'' +
                ", customerId='" + customerId + '\'' +
                ", alertType='" + alertType + '\'' +
                ", severity='" + severity + '\'' +
                ", confidenceScore=" + confidenceScore +
                ", detectedAt=" + detectedAt +
                '}';
    }
}
