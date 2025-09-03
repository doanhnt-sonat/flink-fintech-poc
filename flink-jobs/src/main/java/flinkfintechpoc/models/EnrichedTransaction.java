package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

/**
 * Enriched Transaction with Customer data
 * Used as intermediate result in cascade processing
 */
public class EnrichedTransaction {
    private String id;
    private String transactionType;
    private String status;
    private String fromAccountId;
    private String toAccountId;
    private String customerId;
    private BigDecimal amount;
    private String currency;
    private BigDecimal exchangeRate;
    private BigDecimal feeAmount;
    private String description;
    private String merchantId;
    private String referenceNumber;
    private String authorizationCode;
    private Map<String, Object> transactionLocation;
    private String deviceFingerprint;
    private String ipAddress;
    private String userAgent;
    private BigDecimal riskScore;
    private String riskLevel;
    private Object complianceFlags;
    private Integer processingTimeMs;
    private String network;
    private String cardLastFour;
    private Object tags;
    private Object metadata;
    private Date createdAt;
    private Date updatedAt;
    private Integer version;
    
    // Customer data (enriched)
    private String customerFirstName;
    private String customerLastName;
    private String customerEmail;
    private String customerTier;
    private String customerKycStatus;
    private BigDecimal customerRiskScore;
    private BigDecimal customerAnnualIncome;
    private String customerEmploymentStatus;
    
    // Constructors
    public EnrichedTransaction() {}
    
    public EnrichedTransaction(Transaction transaction, Customer customer) {
        // Copy transaction data
        this.id = transaction.getId();
        this.transactionType = transaction.getTransactionType();
        this.status = transaction.getStatus();
        this.fromAccountId = transaction.getFromAccountId();
        this.toAccountId = transaction.getToAccountId();
        this.customerId = transaction.getCustomerId();
        this.amount = transaction.getAmount();
        this.currency = transaction.getCurrency();
        this.exchangeRate = transaction.getExchangeRate();
        this.feeAmount = transaction.getFeeAmount();
        this.description = transaction.getDescription();
        this.merchantId = transaction.getMerchantId();
        this.referenceNumber = transaction.getReferenceNumber();
        this.authorizationCode = transaction.getAuthorizationCode();
        this.transactionLocation = transaction.getTransactionLocation();
        this.deviceFingerprint = transaction.getDeviceFingerprint();
        this.ipAddress = transaction.getIpAddress();
        this.userAgent = transaction.getUserAgent();
        this.riskScore = BigDecimal.valueOf(transaction.getRiskScore());
        this.riskLevel = transaction.getRiskLevel();
        this.complianceFlags = transaction.getComplianceFlags();
        this.processingTimeMs = transaction.getProcessingTimeMs();
        this.network = transaction.getNetwork();
        this.cardLastFour = transaction.getCardLastFour();
        this.tags = transaction.getTags();
        this.metadata = transaction.getMetadata();
        this.createdAt = transaction.getCreatedAt();
        this.updatedAt = transaction.getUpdatedAt();
        this.version = 1; // Default version
        
        // Enrich with customer data
        if (customer != null) {
            this.customerFirstName = customer.getFirstName();
            this.customerLastName = customer.getLastName();
            this.customerEmail = customer.getEmail();
            this.customerTier = customer.getTier();
            this.customerKycStatus = customer.getKycStatus();
            this.customerRiskScore = BigDecimal.valueOf(customer.getRiskScore());
            this.customerAnnualIncome = customer.getAnnualIncome();
            this.customerEmploymentStatus = customer.getEmploymentStatus();
        }
    }
    
    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getTransactionType() { return transactionType; }
    public void setTransactionType(String transactionType) { this.transactionType = transactionType; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    public String getFromAccountId() { return fromAccountId; }
    public void setFromAccountId(String fromAccountId) { this.fromAccountId = fromAccountId; }
    
    public String getToAccountId() { return toAccountId; }
    public void setToAccountId(String toAccountId) { this.toAccountId = toAccountId; }
    
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    
    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }
    
    public BigDecimal getExchangeRate() { return exchangeRate; }
    public void setExchangeRate(BigDecimal exchangeRate) { this.exchangeRate = exchangeRate; }
    
    public BigDecimal getFeeAmount() { return feeAmount; }
    public void setFeeAmount(BigDecimal feeAmount) { this.feeAmount = feeAmount; }
    
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    
    public String getMerchantId() { return merchantId; }
    public void setMerchantId(String merchantId) { this.merchantId = merchantId; }
    
    public String getReferenceNumber() { return referenceNumber; }
    public void setReferenceNumber(String referenceNumber) { this.referenceNumber = referenceNumber; }
    
    public String getAuthorizationCode() { return authorizationCode; }
    public void setAuthorizationCode(String authorizationCode) { this.authorizationCode = authorizationCode; }
    
    public Map<String, Object> getTransactionLocation() { return transactionLocation; }
    public void setTransactionLocation(Map<String, Object> transactionLocation) { this.transactionLocation = transactionLocation; }
    
    public String getDeviceFingerprint() { return deviceFingerprint; }
    public void setDeviceFingerprint(String deviceFingerprint) { this.deviceFingerprint = deviceFingerprint; }
    
    public String getIpAddress() { return ipAddress; }
    public void setIpAddress(String ipAddress) { this.ipAddress = ipAddress; }
    
    public String getUserAgent() { return userAgent; }
    public void setUserAgent(String userAgent) { this.userAgent = userAgent; }
    
    public BigDecimal getRiskScore() { return riskScore; }
    public void setRiskScore(BigDecimal riskScore) { this.riskScore = riskScore; }
    
    public String getRiskLevel() { return riskLevel; }
    public void setRiskLevel(String riskLevel) { this.riskLevel = riskLevel; }
    
    public Object getComplianceFlags() { return complianceFlags; }
    public void setComplianceFlags(Object complianceFlags) { this.complianceFlags = complianceFlags; }
    
    public Integer getProcessingTimeMs() { return processingTimeMs; }
    public void setProcessingTimeMs(Integer processingTimeMs) { this.processingTimeMs = processingTimeMs; }
    
    public String getNetwork() { return network; }
    public void setNetwork(String network) { this.network = network; }
    
    public String getCardLastFour() { return cardLastFour; }
    public void setCardLastFour(String cardLastFour) { this.cardLastFour = cardLastFour; }
    
    public Object getTags() { return tags; }
    public void setTags(Object tags) { this.tags = tags; }
    
    public Object getMetadata() { return metadata; }
    public void setMetadata(Object metadata) { this.metadata = metadata; }
    
    public Date getCreatedAt() { return createdAt; }
    public void setCreatedAt(Date createdAt) { this.createdAt = createdAt; }
    
    public Date getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Date updatedAt) { this.updatedAt = updatedAt; }
    
    public Integer getVersion() { return version; }
    public void setVersion(Integer version) { this.version = version; }
    
    // Customer data getters and setters
    public String getCustomerFirstName() { return customerFirstName; }
    public void setCustomerFirstName(String customerFirstName) { this.customerFirstName = customerFirstName; }
    
    public String getCustomerLastName() { return customerLastName; }
    public void setCustomerLastName(String customerLastName) { this.customerLastName = customerLastName; }
    
    public String getCustomerEmail() { return customerEmail; }
    public void setCustomerEmail(String customerEmail) { this.customerEmail = customerEmail; }
    
    public String getCustomerTier() { return customerTier; }
    public void setCustomerTier(String customerTier) { this.customerTier = customerTier; }
    
    public String getCustomerKycStatus() { return customerKycStatus; }
    public void setCustomerKycStatus(String customerKycStatus) { this.customerKycStatus = customerKycStatus; }
    
    public BigDecimal getCustomerRiskScore() { return customerRiskScore; }
    public void setCustomerRiskScore(BigDecimal customerRiskScore) { this.customerRiskScore = customerRiskScore; }
    
    public BigDecimal getCustomerAnnualIncome() { return customerAnnualIncome; }
    public void setCustomerAnnualIncome(BigDecimal customerAnnualIncome) { this.customerAnnualIncome = customerAnnualIncome; }
    
    public String getCustomerEmploymentStatus() { return customerEmploymentStatus; }
    public void setCustomerEmploymentStatus(String customerEmploymentStatus) { this.customerEmploymentStatus = customerEmploymentStatus; }
}
