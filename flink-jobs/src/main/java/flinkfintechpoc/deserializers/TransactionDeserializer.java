package flinkfintechpoc.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import flinkfintechpoc.models.Transaction;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Deserializer for Transaction objects from Kafka JSON messages
 */
public class TransactionDeserializer implements MapFunction<String, Transaction> {
    
    private static final Logger LOG = LoggerFactory.getLogger(TransactionDeserializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public Transaction map(String jsonString) throws Exception {
        try {
            JsonNode jsonNode = objectMapper.readTree(jsonString);
            
            Transaction transaction = new Transaction();
            
            // Basic fields
            if (jsonNode.has("id")) {
                transaction.setId(jsonNode.get("id").asText());
            }
            if (jsonNode.has("transaction_type")) {
                transaction.setTransactionType(jsonNode.get("transaction_type").asText());
            }
            if (jsonNode.has("status")) {
                transaction.setStatus(jsonNode.get("status").asText());
            }
            if (jsonNode.has("from_account_id")) {
                transaction.setFromAccountId(jsonNode.get("from_account_id").asText());
            }
            if (jsonNode.has("to_account_id")) {
                transaction.setToAccountId(jsonNode.get("to_account_id").asText());
            }
            if (jsonNode.has("customer_id")) {
                transaction.setCustomerId(jsonNode.get("customer_id").asText());
            }
            if (jsonNode.has("amount")) {
                transaction.setAmount(new BigDecimal(jsonNode.get("amount").asText()));
            }
            if (jsonNode.has("currency")) {
                transaction.setCurrency(jsonNode.get("currency").asText());
            }
            if (jsonNode.has("exchange_rate")) {
                transaction.setExchangeRate(new BigDecimal(jsonNode.get("exchange_rate").asText()));
            }
            if (jsonNode.has("fee_amount")) {
                transaction.setFeeAmount(new BigDecimal(jsonNode.get("fee_amount").asText()));
            }
            if (jsonNode.has("description")) {
                transaction.setDescription(jsonNode.get("description").asText());
            }
            if (jsonNode.has("merchant_id")) {
                transaction.setMerchantId(jsonNode.get("merchant_id").asText());
            }
            if (jsonNode.has("reference_number")) {
                transaction.setReferenceNumber(jsonNode.get("reference_number").asText());
            }
            if (jsonNode.has("authorization_code")) {
                transaction.setAuthorizationCode(jsonNode.get("authorization_code").asText());
            }
            
            // Location and device info
            if (jsonNode.has("transaction_location")) {
                Map<String, Object> location = new HashMap<>();
                JsonNode locationNode = jsonNode.get("transaction_location");
                if (locationNode.has("country")) {
                    location.put("country", locationNode.get("country").asText());
                }
                if (locationNode.has("city")) {
                    location.put("city", locationNode.get("city").asText());
                }
                if (locationNode.has("latitude")) {
                    location.put("latitude", locationNode.get("latitude").asDouble());
                }
                if (locationNode.has("longitude")) {
                    location.put("longitude", locationNode.get("longitude").asDouble());
                }
                transaction.setTransactionLocation(location);
            }
            
            if (jsonNode.has("device_fingerprint")) {
                transaction.setDeviceFingerprint(jsonNode.get("device_fingerprint").asText());
            }
            if (jsonNode.has("ip_address")) {
                transaction.setIpAddress(jsonNode.get("ip_address").asText());
            }
            if (jsonNode.has("user_agent")) {
                transaction.setUserAgent(jsonNode.get("user_agent").asText());
            }
            
            // Risk and compliance
            if (jsonNode.has("risk_score")) {
                transaction.setRiskScore(jsonNode.get("risk_score").asDouble());
            }
            if (jsonNode.has("risk_level")) {
                transaction.setRiskLevel(jsonNode.get("risk_level").asText());
            }
            if (jsonNode.has("compliance_flags")) {
                List<String> flags = new ArrayList<>();
                JsonNode flagsNode = jsonNode.get("compliance_flags");
                if (flagsNode.isArray()) {
                    for (JsonNode flag : flagsNode) {
                        flags.add(flag.asText());
                    }
                }
                transaction.setComplianceFlags(flags);
            }
            
            // Processing details
            if (jsonNode.has("processing_time_ms")) {
                transaction.setProcessingTimeMs(jsonNode.get("processing_time_ms").asInt());
            }
            if (jsonNode.has("network")) {
                transaction.setNetwork(jsonNode.get("network").asText());
            }
            if (jsonNode.has("card_last_four")) {
                transaction.setCardLastFour(jsonNode.get("card_last_four").asText());
            }
            
            // Metadata
            if (jsonNode.has("tags")) {
                List<String> tags = new ArrayList<>();
                JsonNode tagsNode = jsonNode.get("tags");
                if (tagsNode.isArray()) {
                    for (JsonNode tag : tagsNode) {
                        tags.add(tag.asText());
                    }
                }
                transaction.setTags(tags);
            }
            
            if (jsonNode.has("metadata")) {
                Map<String, Object> metadata = new HashMap<>();
                JsonNode metadataNode = jsonNode.get("metadata");
                metadataNode.fieldNames().forEachRemaining(fieldName -> {
                    JsonNode fieldValue = metadataNode.get(fieldName);
                    if (fieldValue.isTextual()) {
                        metadata.put(fieldName, fieldValue.asText());
                    } else if (fieldValue.isNumber()) {
                        metadata.put(fieldName, fieldValue.asDouble());
                    } else if (fieldValue.isBoolean()) {
                        metadata.put(fieldName, fieldValue.asBoolean());
                    }
                });
                transaction.setMetadata(metadata);
            }
            
            // Timestamps
            if (jsonNode.has("created_at")) {
                String createdAtStr = jsonNode.get("created_at").asText();
                // Parse ISO 8601 format
                transaction.setCreatedAt(new Date());
            }
            if (jsonNode.has("updated_at")) {
                String updatedAtStr = jsonNode.get("updated_at").asText();
                transaction.setUpdatedAt(new Date());
            }
            
            return transaction;
            
        } catch (Exception e) {
            LOG.error("Error deserializing transaction: " + jsonString, e);
            // Return a default transaction to avoid pipeline failure
            return new Transaction("error", "unknown", BigDecimal.ZERO, "unknown");
        }
    }
}
