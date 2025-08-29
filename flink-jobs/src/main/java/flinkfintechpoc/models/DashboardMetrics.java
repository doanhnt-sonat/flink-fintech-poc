package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

/**
 * Real-time dashboard metrics for Grafana
 */
public class DashboardMetrics {
    private String metricType;
    private String transactionType;
    private Date timestamp;
    private long transactionCount;
    private BigDecimal totalAmount;
    private BigDecimal averageAmount;
    private double fraudRate;
    private double averageRiskScore;
    private Map<String, Object> additionalMetrics;
    
    // Constructors
    public DashboardMetrics() {}
    
    public DashboardMetrics(String metricType, String transactionType, Date timestamp) {
        this.metricType = metricType;
        this.transactionType = transactionType;
        this.timestamp = timestamp;
        this.transactionCount = 0;
        this.totalAmount = BigDecimal.ZERO;
        this.averageAmount = BigDecimal.ZERO;
        this.fraudRate = 0.0;
        this.averageRiskScore = 0.0;
    }
    
    // Getters and Setters
    public String getMetricType() { return metricType; }
    public void setMetricType(String metricType) { this.metricType = metricType; }
    
    public String getTransactionType() { return transactionType; }
    public void setTransactionType(String transactionType) { this.transactionType = transactionType; }
    
    public Date getTimestamp() { return timestamp; }
    public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
    
    public long getTransactionCount() { return transactionCount; }
    public void setTransactionCount(long transactionCount) { this.transactionCount = transactionCount; }
    
    public BigDecimal getTotalAmount() { return totalAmount; }
    public void setTotalAmount(BigDecimal totalAmount) { this.totalAmount = totalAmount; }
    
    public BigDecimal getAverageAmount() { return averageAmount; }
    public void setAverageAmount(BigDecimal averageAmount) { this.averageAmount = averageAmount; }
    
    public double getFraudRate() { return fraudRate; }
    public void setFraudRate(double fraudRate) { this.fraudRate = fraudRate; }
    
    public double getAverageRiskScore() { return averageRiskScore; }
    public void setAverageRiskScore(double averageRiskScore) { this.averageRiskScore = averageRiskScore; }
    
    public Map<String, Object> getAdditionalMetrics() { return additionalMetrics; }
    public void setAdditionalMetrics(Map<String, Object> additionalMetrics) { this.additionalMetrics = additionalMetrics; }
    
    @Override
    public String toString() {
        return "DashboardMetrics{" +
                "metricType='" + metricType + '\'' +
                ", transactionType='" + transactionType + '\'' +
                ", timestamp=" + timestamp +
                ", transactionCount=" + transactionCount +
                ", totalAmount=" + totalAmount +
                ", fraudRate=" + fraudRate +
                '}';
    }
}
