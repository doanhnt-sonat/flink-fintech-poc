package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonNaming;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

/**
 * Transaction model for Flink processing
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class Transaction {
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
    
    // Location and device info
    @JsonDeserialize(using = CustomerSession.LocationDeserializer.class)
    private Map<String, Object> transactionLocation;
    private String deviceFingerprint;
    private String ipAddress;
    private String userAgent;
    
    // Risk and compliance
    private double riskScore;
    private String riskLevel;
    @JsonDeserialize(using = StringOrArrayStringListDeserializer.class)
    private List<String> complianceFlags;
    
    // Processing details
    private Integer processingTimeMs;
    private String network;
    private String cardLastFour;
    
    // Metadata
    @JsonDeserialize(using = StringOrArrayStringListDeserializer.class)
    private List<String> tags;
    @JsonDeserialize(using = CustomerSession.LocationDeserializer.class)
    private Map<String, Object> metadata;
    
    // Timestamps
    private Date createdAt;
    private Date updatedAt;
    
    // Constructors
    public Transaction() {}
    
    public Transaction(String id, String customerId, BigDecimal amount, String transactionType) {
        this.id = id;
        this.customerId = customerId;
        this.amount = amount;
        this.transactionType = transactionType;
        this.createdAt = new Date();
        this.updatedAt = new Date();
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
    
    public double getRiskScore() { return riskScore; }
    public void setRiskScore(double riskScore) { this.riskScore = riskScore; }
    
    public String getRiskLevel() { return riskLevel; }
    public void setRiskLevel(String riskLevel) { this.riskLevel = riskLevel; }
    
    public List<String> getComplianceFlags() { return complianceFlags; }
    public void setComplianceFlags(List<String> complianceFlags) { this.complianceFlags = complianceFlags; }
    
    public Integer getProcessingTimeMs() { return processingTimeMs; }
    public void setProcessingTimeMs(Integer processingTimeMs) { this.processingTimeMs = processingTimeMs; }
    
    public String getNetwork() { return network; }
    public void setNetwork(String network) { this.network = network; }
    
    public String getCardLastFour() { return cardLastFour; }
    public void setCardLastFour(String cardLastFour) { this.cardLastFour = cardLastFour; }
    
    public List<String> getTags() { return tags; }
    public void setTags(List<String> tags) { this.tags = tags; }
    
    public Map<String, Object> getMetadata() { return metadata; }
    public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }
    
    public Date getCreatedAt() { return createdAt; }
    public void setCreatedAt(Date createdAt) { this.createdAt = createdAt; }
    
    public Date getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Date updatedAt) { this.updatedAt = updatedAt; }
    
    @Override
    public String toString() {
        return "Transaction{" +
                "id='" + id + '\'' +
                ", customerId='" + customerId + '\'' +
                ", amount=" + amount +
                ", transactionType='" + transactionType + '\'' +
                ", status='" + status + '\'' +
                ", createdAt=" + createdAt +
                '}';
    }

    /**
     * Deserializer that accepts JSON array of strings or a stringified JSON array,
     * and returns a List<String>.
     */
    public static class StringOrArrayStringListDeserializer extends org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer<List<String>> {
        private static final ObjectMapper M = new ObjectMapper();
        private static final TypeReference<List<String>> T = new TypeReference<List<String>>() {};

        @Override
        public List<String> deserialize(JsonParser p, DeserializationContext ctxt) {
            try {
                JsonNode n = p.getCodec().readTree(p);
                if (n == null || n.isNull()) return null;
                if (n.isArray()) return M.convertValue(n, T);
                if (n.isTextual()) {
                    String s = n.asText();
                    if (s == null || s.isEmpty()) return null;
                    return M.readValue(s, T);
                }
                return M.convertValue(n, T);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize compliance_flags", e);
            }
        }
    }
}
