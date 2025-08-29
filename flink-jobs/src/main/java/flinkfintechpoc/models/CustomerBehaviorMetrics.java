package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Customer behavior metrics for analytics
 */
public class CustomerBehaviorMetrics {
    private String customerId;
    private Date calculationDate;
    private BigDecimal totalBalance;
    private BigDecimal monthlySpending;
    private BigDecimal monthlyIncome;
    private double transactionFrequency;
    private double riskScore;
    private double engagementScore;
    private BigDecimal lifetimeValue;
    private List<String> preferredTransactionTypes;
    private Map<String, Integer> locationPatterns;
    private Map<String, Integer> devicePatterns;
    private double averageTransactionAmount;
    private int totalTransactions;
    private String customerSegment;
    
    // Constructors
    public CustomerBehaviorMetrics() {}
    
    public CustomerBehaviorMetrics(String customerId, Date calculationDate) {
        this.customerId = customerId;
        this.calculationDate = calculationDate;
        this.totalBalance = BigDecimal.ZERO;
        this.monthlySpending = BigDecimal.ZERO;
        this.monthlyIncome = BigDecimal.ZERO;
        this.transactionFrequency = 0.0;
        this.riskScore = 0.0;
        this.engagementScore = 0.0;
        this.lifetimeValue = BigDecimal.ZERO;
        this.averageTransactionAmount = 0.0;
        this.totalTransactions = 0;
    }
    
    // Getters and Setters
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public Date getCalculationDate() { return calculationDate; }
    public void setCalculationDate(Date calculationDate) { this.calculationDate = calculationDate; }
    
    public BigDecimal getTotalBalance() { return totalBalance; }
    public void setTotalBalance(BigDecimal totalBalance) { this.totalBalance = totalBalance; }
    
    public BigDecimal getMonthlySpending() { return monthlySpending; }
    public void setMonthlySpending(BigDecimal monthlySpending) { this.monthlySpending = monthlySpending; }
    
    public BigDecimal getMonthlyIncome() { return monthlyIncome; }
    public void setMonthlyIncome(BigDecimal monthlyIncome) { this.monthlyIncome = monthlyIncome; }
    
    public double getTransactionFrequency() { return transactionFrequency; }
    public void setTransactionFrequency(double transactionFrequency) { this.transactionFrequency = transactionFrequency; }
    
    public double getRiskScore() { return riskScore; }
    public void setRiskScore(double riskScore) { this.riskScore = riskScore; }
    
    public double getEngagementScore() { return engagementScore; }
    public void setEngagementScore(double engagementScore) { this.engagementScore = engagementScore; }
    
    public BigDecimal getLifetimeValue() { return lifetimeValue; }
    public void setLifetimeValue(BigDecimal lifetimeValue) { this.lifetimeValue = lifetimeValue; }
    
    public List<String> getPreferredTransactionTypes() { return preferredTransactionTypes; }
    public void setPreferredTransactionTypes(List<String> preferredTransactionTypes) { this.preferredTransactionTypes = preferredTransactionTypes; }
    
    public Map<String, Integer> getLocationPatterns() { return locationPatterns; }
    public void setLocationPatterns(Map<String, Integer> locationPatterns) { this.locationPatterns = locationPatterns; }
    
    public Map<String, Integer> getDevicePatterns() { return devicePatterns; }
    public void setDevicePatterns(Map<String, Integer> devicePatterns) { this.devicePatterns = devicePatterns; }
    
    public double getAverageTransactionAmount() { return averageTransactionAmount; }
    public void setAverageTransactionAmount(double averageTransactionAmount) { this.averageTransactionAmount = averageTransactionAmount; }
    
    public int getTotalTransactions() { return totalTransactions; }
    public void setTotalTransactions(int totalTransactions) { this.totalTransactions = totalTransactions; }
    
    public String getCustomerSegment() { return customerSegment; }
    public void setCustomerSegment(String customerSegment) { this.customerSegment = customerSegment; }
    
    @Override
    public String toString() {
        return "CustomerBehaviorMetrics{" +
                "customerId='" + customerId + '\'' +
                ", calculationDate=" + calculationDate +
                ", totalBalance=" + totalBalance +
                ", monthlySpending=" + monthlySpending +
                ", riskScore=" + riskScore +
                ", engagementScore=" + engagementScore +
                ", customerSegment='" + customerSegment + '\'' +
                '}';
    }
}
