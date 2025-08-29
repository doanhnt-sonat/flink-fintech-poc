package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Transaction metrics for analytics
 */
public class TransactionMetrics {
    private String customerId;
    private String accountId;
    private Date windowStart;
    private Date windowEnd;
    private int transactionCount;
    private BigDecimal totalAmount;
    private BigDecimal totalCredits;
    private BigDecimal totalDebits;
    private BigDecimal averageAmount;
    private BigDecimal largestTransaction;
    private BigDecimal smallestTransaction;
    private String mostCommonType;
    private int fraudCount;
    private double averageRiskScore;
    
    // Constructors
    public TransactionMetrics() {}
    
    public TransactionMetrics(String customerId, Date windowStart, Date windowEnd) {
        this.customerId = customerId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.transactionCount = 0;
        this.totalAmount = BigDecimal.ZERO;
        this.totalCredits = BigDecimal.ZERO;
        this.totalDebits = BigDecimal.ZERO;
        this.averageAmount = BigDecimal.ZERO;
        this.largestTransaction = BigDecimal.ZERO;
        this.smallestTransaction = BigDecimal.ZERO;
        this.fraudCount = 0;
        this.averageRiskScore = 0.0;
    }
    
    // Getters and Setters
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public String getAccountId() { return accountId; }
    public void setAccountId(String accountId) { this.accountId = accountId; }
    
    public Date getWindowStart() { return windowStart; }
    public void setWindowStart(Date windowStart) { this.windowStart = windowStart; }
    
    public Date getWindowEnd() { return windowEnd; }
    public void setWindowEnd(Date windowEnd) { this.windowEnd = windowEnd; }
    
    public int getTransactionCount() { return transactionCount; }
    public void setTransactionCount(int transactionCount) { this.transactionCount = transactionCount; }
    
    public BigDecimal getTotalAmount() { return totalAmount; }
    public void setTotalAmount(BigDecimal totalAmount) { this.totalAmount = totalAmount; }
    
    public BigDecimal getTotalCredits() { return totalCredits; }
    public void setTotalCredits(BigDecimal totalCredits) { this.totalCredits = totalCredits; }
    
    public BigDecimal getTotalDebits() { return totalDebits; }
    public void setTotalDebits(BigDecimal totalDebits) { this.totalDebits = totalDebits; }
    
    public BigDecimal getAverageAmount() { return averageAmount; }
    public void setAverageAmount(BigDecimal averageAmount) { this.averageAmount = averageAmount; }
    
    public BigDecimal getLargestTransaction() { return largestTransaction; }
    public void setLargestTransaction(BigDecimal largestTransaction) { this.largestTransaction = largestTransaction; }
    
    public BigDecimal getSmallestTransaction() { return smallestTransaction; }
    public void setSmallestTransaction(BigDecimal smallestTransaction) { this.smallestTransaction = smallestTransaction; }
    
    public String getMostCommonType() { return mostCommonType; }
    public void setMostCommonType(String mostCommonType) { this.mostCommonType = mostCommonType; }
    
    public int getFraudCount() { return fraudCount; }
    public void setFraudCount(int fraudCount) { this.fraudCount = fraudCount; }
    
    public double getAverageRiskScore() { return averageRiskScore; }
    public void setAverageRiskScore(double averageRiskScore) { this.averageRiskScore = averageRiskScore; }
    
    @Override
    public String toString() {
        return "TransactionMetrics{" +
                "customerId='" + customerId + '\'' +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", transactionCount=" + transactionCount +
                ", totalAmount=" + totalAmount +
                ", averageAmount=" + averageAmount +
                ", fraudCount=" + fraudCount +
                '}';
    }
}
