package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

/**
 * Merchant transaction metrics
 */
public class MerchantMetrics {
    private String merchantId;
    private String merchantName;
    private String businessType;
    private Date windowStart;
    private Date windowEnd;
    private long transactionCount;
    private BigDecimal totalAmount;
    private BigDecimal averageAmount;
    private double fraudRate;
    private double averageRiskScore;
    private Map<String, Object> merchantDetails;
    
    // Constructors
    public MerchantMetrics() {}
    
    public MerchantMetrics(String merchantId, Date windowStart, Date windowEnd) {
        this.merchantId = merchantId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.transactionCount = 0;
        this.totalAmount = BigDecimal.ZERO;
        this.averageAmount = BigDecimal.ZERO;
        this.fraudRate = 0.0;
        this.averageRiskScore = 0.0;
    }
    
    // Getters and Setters
    public String getMerchantId() { return merchantId; }
    public void setMerchantId(String merchantId) { this.merchantId = merchantId; }
    
    public String getMerchantName() { return merchantName; }
    public void setMerchantName(String merchantName) { this.merchantName = merchantName; }
    
    public String getBusinessType() { return businessType; }
    public void setBusinessType(String businessType) { this.businessType = businessType; }
    
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
    
    public Map<String, Object> getMerchantDetails() { return merchantDetails; }
    public void setMerchantDetails(Map<String, Object> merchantDetails) { this.merchantDetails = merchantDetails; }
    
    @Override
    public String toString() {
        return "MerchantMetrics{" +
                "merchantId='" + merchantId + '\'' +
                ", merchantName='" + merchantName + '\'' +
                ", businessType='" + businessType + '\'' +
                ", transactionCount=" + transactionCount +
                ", totalAmount=" + totalAmount +
                ", fraudRate=" + fraudRate +
                '}';
    }
}
