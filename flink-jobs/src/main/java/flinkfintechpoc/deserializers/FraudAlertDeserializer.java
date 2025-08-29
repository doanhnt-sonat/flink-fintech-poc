package flinkfintechpoc.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import flinkfintechpoc.models.FraudAlert;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Deserializer for FraudAlert objects from Kafka JSON messages
 */
public class FraudAlertDeserializer implements MapFunction<String, FraudAlert> {
    
    private static final Logger LOG = LoggerFactory.getLogger(FraudAlertDeserializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public FraudAlert map(String jsonString) throws Exception {
        try {
            JsonNode jsonNode = objectMapper.readTree(jsonString);
            
            FraudAlert fraudAlert = new FraudAlert();
            
            // Basic fields
            if (jsonNode.has("id")) {
                fraudAlert.setId(jsonNode.get("id").asText());
            }
            if (jsonNode.has("customer_id")) {
                fraudAlert.setCustomerId(jsonNode.get("customer_id").asText());
            }
            if (jsonNode.has("transaction_id")) {
                fraudAlert.setTransactionId(jsonNode.get("transaction_id").asText());
            }
            if (jsonNode.has("alert_type")) {
                fraudAlert.setAlertType(jsonNode.get("alert_type").asText());
            }
            if (jsonNode.has("severity")) {
                fraudAlert.setSeverity(jsonNode.get("severity").asText());
            }
            if (jsonNode.has("description")) {
                fraudAlert.setDescription(jsonNode.get("description").asText());
            }
            if (jsonNode.has("confidence_score")) {
                fraudAlert.setConfidenceScore(jsonNode.get("confidence_score").asDouble());
            }
            
            // Rules triggered
            if (jsonNode.has("rules_triggered")) {
                List<String> rules = new ArrayList<>();
                JsonNode rulesNode = jsonNode.get("rules_triggered");
                if (rulesNode.isArray()) {
                    for (JsonNode rule : rulesNode) {
                        rules.add(rule.asText());
                    }
                }
                fraudAlert.setRulesTriggered(rules);
            }
            
            if (jsonNode.has("is_resolved")) {
                fraudAlert.setResolved(jsonNode.get("is_resolved").asBoolean());
            }
            if (jsonNode.has("resolution_notes")) {
                fraudAlert.setResolutionNotes(jsonNode.get("resolution_notes").asText());
            }
            
            // Timestamps
            if (jsonNode.has("created_at")) {
                String createdAtStr = jsonNode.get("created_at").asText();
                fraudAlert.setCreatedAt(new Date());
            }
            if (jsonNode.has("updated_at")) {
                String updatedAtStr = jsonNode.get("updated_at").asText();
                fraudAlert.setUpdatedAt(new Date());
            }
            
            return fraudAlert;
            
        } catch (Exception e) {
            LOG.error("Error deserializing fraud alert: " + jsonString, e);
            // Return a default fraud alert to avoid pipeline failure
            return new FraudAlert("error", "unknown", "unknown", "low", "Error parsing fraud alert");
        }
    }
}
