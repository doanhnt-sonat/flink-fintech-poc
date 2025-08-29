package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Account model for Flink processing
 */
public class Account {
    private String id;
    private String customerId;
    private String accountNumber;
    private String accountType;
    private String currency;
    private BigDecimal interestRate;
    private boolean isActive;
    private boolean isFrozen;
    private boolean overdraftProtection;
    private BigDecimal minimumBalance;
    private BigDecimal monthlyFee;
    private String branchCode;
    private String routingNumber;
    private Date createdAt;
    private Date updatedAt;
    
    // Constructors
    public Account() {}
    
    public Account(String id, String customerId, String accountNumber, String accountType) {
        this.id = id;
        this.customerId = customerId;
        this.accountNumber = accountNumber;
        this.accountType = accountType;
        this.currency = "USD";
        this.isActive = true;
        this.isFrozen = false;
        this.createdAt = new Date();
        this.updatedAt = new Date();
    }
    
    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public String getAccountNumber() { return accountNumber; }
    public void setAccountNumber(String accountNumber) { this.accountNumber = accountNumber; }
    
    public String getAccountType() { return accountType; }
    public void setAccountType(String accountType) { this.accountType = accountType; }
    
    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }
    
    public BigDecimal getInterestRate() { return interestRate; }
    public void setInterestRate(BigDecimal interestRate) { this.interestRate = interestRate; }
    
    public boolean isActive() { return isActive; }
    public void setActive(boolean active) { isActive = active; }
    
    public boolean isFrozen() { return isFrozen; }
    public void setFrozen(boolean frozen) { isFrozen = frozen; }
    
    public boolean isOverdraftProtection() { return overdraftProtection; }
    public void setOverdraftProtection(boolean overdraftProtection) { this.overdraftProtection = overdraftProtection; }
    
    public BigDecimal getMinimumBalance() { return minimumBalance; }
    public void setMinimumBalance(BigDecimal minimumBalance) { this.minimumBalance = minimumBalance; }
    
    public BigDecimal getMonthlyFee() { return monthlyFee; }
    public void setMonthlyFee(BigDecimal monthlyFee) { this.monthlyFee = monthlyFee; }
    
    public String getBranchCode() { return branchCode; }
    public void setBranchCode(String branchCode) { this.branchCode = branchCode; }
    
    public String getRoutingNumber() { return routingNumber; }
    public void setRoutingNumber(String routingNumber) { this.routingNumber = routingNumber; }
    
    public Date getCreatedAt() { return createdAt; }
    public void setCreatedAt(Date createdAt) { this.createdAt = createdAt; }
    
    public Date getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Date updatedAt) { this.updatedAt = updatedAt; }
    
    @Override
    public String toString() {
        return "Account{" +
                "id='" + id + '\'' +
                ", customerId='" + customerId + '\'' +
                ", accountNumber='" + accountNumber + '\'' +
                ", accountType='" + accountType + '\'' +
                ", currency='" + currency + '\'' +
                ", isActive=" + isActive +
                ", createdAt=" + createdAt +
                '}';
    }
}
