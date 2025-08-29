package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Compliance monitoring metrics
 */
public class ComplianceMetrics {
    private String customerId;
    private Date windowStart;
    private Date windowEnd;
    private int complianceAlerts;
    private List<String> complianceFlags;
    private double complianceRiskScore;
    private String complianceStatus;
    private Map<String, Object> complianceDetails;
    private BigDecimal totalExposure;
    private int suspiciousTransactions;
    
    // Constructors
    public ComplianceMetrics() {}
    
    public ComplianceMetrics(String customerId, Date windowStart, Date windowEnd) {
        this.customerId = customerId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.complianceAlerts = 0;
        this.complianceRiskScore = 0.0;
        this.complianceStatus = "compliant";
        this.totalExposure = BigDecimal.ZERO;
        this.suspiciousTransactions = 0;
    }
    
    // Getters and Setters
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public Date getWindowStart() { return windowStart; }
    public void setWindowStart(Date windowStart) { this.windowStart = windowStart; }
    
    public Date getWindowEnd() { return windowEnd; }
    public void setWindowEnd(Date windowEnd) { this.windowEnd = windowEnd; }
    
    public int getComplianceAlerts() { return complianceAlerts; }
    public void setComplianceAlerts(int complianceAlerts) { this.complianceAlerts = complianceAlerts; }
    
    public List<String> getComplianceFlags() { return complianceFlags; }
    public void setComplianceFlags(List<String> complianceFlags) { this.complianceFlags = complianceFlags; }
    
    public double getComplianceRiskScore() { return complianceRiskScore; }
    public void setComplianceRiskScore(double complianceRiskScore) { this.complianceRiskScore = complianceRiskScore; }
    
    public String getComplianceStatus() { return complianceStatus; }
    public void setComplianceStatus(String complianceStatus) { this.complianceStatus = complianceStatus; }
    
    public Map<String, Object> getComplianceDetails() { return complianceDetails; }
    public void setComplianceDetails(Map<String, Object> complianceDetails) { this.complianceDetails = complianceDetails; }
    
    public BigDecimal getTotalExposure() { return totalExposure; }
    public void setTotalExposure(BigDecimal totalExposure) { this.totalExposure = totalExposure; }
    
    public int getSuspiciousTransactions() { return suspiciousTransactions; }
    public void setSuspiciousTransactions(int suspiciousTransactions) { this.suspiciousTransactions = suspiciousTransactions; }
    
    @Override
    public String toString() {
        return "ComplianceMetrics{" +
                "customerId='" + customerId + '\'' +
                ", complianceAlerts=" + complianceAlerts +
                ", complianceRiskScore=" + complianceRiskScore +
                ", complianceStatus='" + complianceStatus + '\'' +
                ", suspiciousTransactions=" + suspiciousTransactions +
                '}';
    }
}
