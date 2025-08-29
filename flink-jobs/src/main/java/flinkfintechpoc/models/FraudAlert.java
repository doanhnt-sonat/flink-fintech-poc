package flinkfintechpoc.models;

import java.util.Date;
import java.util.List;

/**
 * Fraud Alert model for Flink processing
 */
public class FraudAlert {
    private String id;
    private String customerId;
    private String transactionId;
    private String alertType;
    private String severity;
    private String description;
    private double confidenceScore;
    private List<String> rulesTriggered;
    private boolean isResolved;
    private String resolutionNotes;
    private Date createdAt;
    private Date updatedAt;
    
    // Constructors
    public FraudAlert() {}
    
    public FraudAlert(String id, String customerId, String alertType, String severity, String description) {
        this.id = id;
        this.customerId = customerId;
        this.alertType = alertType;
        this.severity = severity;
        this.description = description;
        this.createdAt = new Date();
        this.updatedAt = new Date();
    }
    
    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    
    public String getAlertType() { return alertType; }
    public void setAlertType(String alertType) { this.alertType = alertType; }
    
    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }
    
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    
    public double getConfidenceScore() { return confidenceScore; }
    public void setConfidenceScore(double confidenceScore) { this.confidenceScore = confidenceScore; }
    
    public List<String> getRulesTriggered() { return rulesTriggered; }
    public void setRulesTriggered(List<String> rulesTriggered) { this.rulesTriggered = rulesTriggered; }
    
    public boolean isResolved() { return isResolved; }
    public void setResolved(boolean resolved) { isResolved = resolved; }
    
    public String getResolutionNotes() { return resolutionNotes; }
    public void setResolutionNotes(String resolutionNotes) { this.resolutionNotes = resolutionNotes; }
    
    public Date getCreatedAt() { return createdAt; }
    public void setCreatedAt(Date createdAt) { this.createdAt = createdAt; }
    
    public Date getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Date updatedAt) { this.updatedAt = updatedAt; }
    
    @Override
    public String toString() {
        return "FraudAlert{" +
                "id='" + id + '\'' +
                ", customerId='" + customerId + '\'' +
                ", alertType='" + alertType + '\'' +
                ", severity='" + severity + '\'' +
                ", confidenceScore=" + confidenceScore +
                ", createdAt=" + createdAt +
                '}';
    }
}
