package flinkfintechpoc.models;

import java.util.Date;

/**
 * Merchant Analytics Metrics model
 */
public class MerchantAnalyticsMetrics {
    private String merchantId;
    private String eventType;
    private int transactionCount;
    private double totalAmount;
    private double averageAmount;
    private Date eventTime;
    private String performanceLevel;
    private String riskLevel;
    private String businessType;
    private String mccCode;
    private boolean isActive;
    
    // Constructors
    public MerchantAnalyticsMetrics() {}
    
    public MerchantAnalyticsMetrics(String merchantId, String eventType, int transactionCount,
                                  double totalAmount, double averageAmount, Date eventTime,
                                  String performanceLevel, String riskLevel) {
        this.merchantId = merchantId;
        this.eventType = eventType;
        this.transactionCount = transactionCount;
        this.totalAmount = totalAmount;
        this.averageAmount = averageAmount;
        this.eventTime = eventTime;
        this.performanceLevel = performanceLevel;
        this.riskLevel = riskLevel;
    }
    
    // Getters and Setters
    public String getMerchantId() { return merchantId; }
    public void setMerchantId(String merchantId) { this.merchantId = merchantId; }
    
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    
    public int getTransactionCount() { return transactionCount; }
    public void setTransactionCount(int transactionCount) { this.transactionCount = transactionCount; }
    
    public double getTotalAmount() { return totalAmount; }
    public void setTotalAmount(double totalAmount) { this.totalAmount = totalAmount; }
    
    public double getAverageAmount() { return averageAmount; }
    public void setAverageAmount(double averageAmount) { this.averageAmount = averageAmount; }
    
    public Date getEventTime() { return eventTime; }
    public void setEventTime(Date eventTime) { this.eventTime = eventTime; }
    
    public String getPerformanceLevel() { return performanceLevel; }
    public void setPerformanceLevel(String performanceLevel) { this.performanceLevel = performanceLevel; }
    
    public String getRiskLevel() { return riskLevel; }
    public void setRiskLevel(String riskLevel) { this.riskLevel = riskLevel; }
    
    public String getBusinessType() { return businessType; }
    public void setBusinessType(String businessType) { this.businessType = businessType; }
    
    public String getMccCode() { return mccCode; }
    public void setMccCode(String mccCode) { this.mccCode = mccCode; }
    
    public boolean isActive() { return isActive; }
    public void setActive(boolean active) { isActive = active; }
    
    @Override
    public String toString() {
        return "MerchantAnalyticsMetrics{" +
                "merchantId='" + merchantId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", transactionCount=" + transactionCount +
                ", totalAmount=" + totalAmount +
                ", averageAmount=" + averageAmount +
                ", eventTime=" + eventTime +
                ", performanceLevel='" + performanceLevel + '\'' +
                ", riskLevel='" + riskLevel + '\'' +
                '}';
    }
}
