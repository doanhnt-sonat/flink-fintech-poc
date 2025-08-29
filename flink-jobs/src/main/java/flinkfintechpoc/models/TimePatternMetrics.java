package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

/**
 * Time-based pattern metrics
 */
public class TimePatternMetrics {
    private int hourOfDay;
    private Date windowStart;
    private Date windowEnd;
    private long transactionCount;
    private BigDecimal totalAmount;
    private BigDecimal averageAmount;
    private double fraudRate;
    private double averageRiskScore;
    private Map<String, Object> timePatternDetails;
    
    // Constructors
    public TimePatternMetrics() {}
    
    public TimePatternMetrics(int hourOfDay, Date windowStart, Date windowEnd) {
        this.hourOfDay = hourOfDay;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.transactionCount = 0;
        this.totalAmount = BigDecimal.ZERO;
        this.averageAmount = BigDecimal.ZERO;
        this.fraudRate = 0.0;
        this.averageRiskScore = 0.0;
    }
    
    // Getters and Setters
    public int getHourOfDay() { return hourOfDay; }
    public void setHourOfDay(int hourOfDay) { this.hourOfDay = hourOfDay; }
    
    public Date getWindowStart() { return windowStart; }
    public void setWindowStart(Date windowStart) { this.windowStart = windowStart; }
    
    public Date getWindowEnd() { return windowEnd; }
    public void setWindowEnd(Date windowEnd) { this.windowEnd = windowEnd; }
    
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
    
    public Map<String, Object> getTimePatternDetails() { return timePatternDetails; }
    public void setTimePatternDetails(Map<String, Object> timePatternDetails) { this.timePatternDetails = timePatternDetails; }
    
    @Override
    public String toString() {
        return "TimePatternMetrics{" +
                "hourOfDay=" + hourOfDay +
                ", transactionCount=" + transactionCount +
                ", totalAmount=" + totalAmount +
                ", fraudRate=" + fraudRate +
                '}';
    }
}
