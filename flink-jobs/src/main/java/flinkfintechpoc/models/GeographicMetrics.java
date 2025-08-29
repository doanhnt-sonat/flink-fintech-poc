package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

/**
 * Geographic transaction metrics
 */
public class GeographicMetrics {
    private String country;
    private String region;
    private Date windowStart;
    private Date windowEnd;
    private long transactionCount;
    private BigDecimal totalAmount;
    private BigDecimal averageAmount;
    private double fraudRate;
    private double averageRiskScore;
    private Map<String, Object> locationDetails;
    
    // Constructors
    public GeographicMetrics() {}
    
    public GeographicMetrics(String country, Date windowStart, Date windowEnd) {
        this.country = country;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.transactionCount = 0;
        this.totalAmount = BigDecimal.ZERO;
        this.averageAmount = BigDecimal.ZERO;
        this.fraudRate = 0.0;
        this.averageRiskScore = 0.0;
    }
    
    // Getters and Setters
    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }
    
    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }
    
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
    
    public Map<String, Object> getLocationDetails() { return locationDetails; }
    public void setLocationDetails(Map<String, Object> locationDetails) { this.locationDetails = locationDetails; }
    
    @Override
    public String toString() {
        return "GeographicMetrics{" +
                "country='" + country + '\'' +
                ", region='" + region + '\'' +
                ", transactionCount=" + transactionCount +
                ", totalAmount=" + totalAmount +
                ", fraudRate=" + fraudRate +
                '}';
    }
}
