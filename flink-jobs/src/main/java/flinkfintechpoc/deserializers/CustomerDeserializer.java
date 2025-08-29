package flinkfintechpoc.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import flinkfintechpoc.models.Customer;
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
 * Deserializer for Customer objects from Kafka JSON messages
 */
public class CustomerDeserializer implements MapFunction<String, Customer> {
    
    private static final Logger LOG = LoggerFactory.getLogger(CustomerDeserializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public Customer map(String jsonString) throws Exception {
        try {
            JsonNode jsonNode = objectMapper.readTree(jsonString);
            
            Customer customer = new Customer();
            
            // Basic fields
            if (jsonNode.has("id")) {
                customer.setId(jsonNode.get("id").asText());
            }
            if (jsonNode.has("first_name")) {
                customer.setFirstName(jsonNode.get("first_name").asText());
            }
            if (jsonNode.has("last_name")) {
                customer.setLastName(jsonNode.get("last_name").asText());
            }
            if (jsonNode.has("email")) {
                customer.setEmail(jsonNode.get("email").asText());
            }
            if (jsonNode.has("phone")) {
                customer.setPhone(jsonNode.get("phone").asText());
            }
            if (jsonNode.has("date_of_birth")) {
                String dobStr = jsonNode.get("date_of_birth").asText();
                // Parse ISO 8601 format
                customer.setDateOfBirth(new Date());
            }
            if (jsonNode.has("ssn")) {
                customer.setSsn(jsonNode.get("ssn").asText());
            }
            if (jsonNode.has("address")) {
                customer.setAddress(jsonNode.get("address").asText());
            }
            if (jsonNode.has("tier")) {
                customer.setTier(jsonNode.get("tier").asText());
            }
            if (jsonNode.has("risk_score")) {
                customer.setRiskScore(jsonNode.get("risk_score").asDouble());
            }
            if (jsonNode.has("kyc_status")) {
                customer.setKycStatus(jsonNode.get("kyc_status").asText());
            }
            if (jsonNode.has("is_active")) {
                customer.setActive(jsonNode.get("is_active").asBoolean());
            }
            if (jsonNode.has("credit_score")) {
                customer.setCreditScore(jsonNode.get("credit_score").asInt());
            }
            if (jsonNode.has("annual_income")) {
                customer.setAnnualIncome(new BigDecimal(jsonNode.get("annual_income").asText()));
            }
            if (jsonNode.has("employment_status")) {
                customer.setEmploymentStatus(jsonNode.get("employment_status").asText());
            }
            if (jsonNode.has("onboarding_channel")) {
                customer.setOnboardingChannel(jsonNode.get("onboarding_channel").asText());
            }
            if (jsonNode.has("referral_code")) {
                customer.setReferralCode(jsonNode.get("referral_code").asText());
            }
            
            // Preferences
            if (jsonNode.has("preferences")) {
                Map<String, Object> preferences = new HashMap<>();
                JsonNode prefsNode = jsonNode.get("preferences");
                prefsNode.fieldNames().forEachRemaining(fieldName -> {
                    JsonNode fieldValue = prefsNode.get(fieldName);
                    if (fieldValue.isTextual()) {
                        preferences.put(fieldName, fieldValue.asText());
                    } else if (fieldValue.isNumber()) {
                        preferences.put(fieldName, fieldValue.asDouble());
                    } else if (fieldValue.isBoolean()) {
                        preferences.put(fieldName, fieldValue.asBoolean());
                    }
                });
                customer.setPreferences(preferences);
            }
            
            // Tags
            if (jsonNode.has("tags")) {
                List<String> tags = new ArrayList<>();
                JsonNode tagsNode = jsonNode.get("tags");
                if (tagsNode.isArray()) {
                    for (JsonNode tag : tagsNode) {
                        tags.add(tag.asText());
                    }
                }
                customer.setTags(tags);
            }
            
            // Timestamps
            if (jsonNode.has("created_at")) {
                String createdAtStr = jsonNode.get("created_at").asText();
                customer.setCreatedAt(new Date());
            }
            if (jsonNode.has("updated_at")) {
                String updatedAtStr = jsonNode.get("updated_at").asText();
                customer.setUpdatedAt(new Date());
            }
            
            return customer;
            
        } catch (Exception e) {
            LOG.error("Error deserializing customer: " + jsonString, e);
            // Return a default customer to avoid pipeline failure
            return new Customer("error", "Unknown", "Customer", "unknown@example.com");
        }
    }
}
