package flinkfintechpoc.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import flinkfintechpoc.models.OutboxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Deserializer for OutboxEvent objects from Kafka JSON messages
 */
public class EventDeserializer implements MapFunction<String, OutboxEvent> {
    
    private static final Logger LOG = LoggerFactory.getLogger(EventDeserializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public OutboxEvent map(String jsonString) throws Exception {
        try {
            JsonNode jsonNode = objectMapper.readTree(jsonString);
            
            OutboxEvent event = new OutboxEvent();
            
            // Basic fields
            if (jsonNode.has("id")) {
                event.setId(jsonNode.get("id").asText());
            }
            if (jsonNode.has("aggregate_type")) {
                event.setAggregateType(jsonNode.get("aggregate_type").asText());
            }
            if (jsonNode.has("aggregate_id")) {
                event.setAggregateId(jsonNode.get("aggregate_id").asText());
            }
            if (jsonNode.has("event_type")) {
                event.setEventType(jsonNode.get("event_type").asText());
            }
            if (jsonNode.has("version")) {
                event.setVersion(jsonNode.get("version").asInt());
            }
            
            // Payload
            if (jsonNode.has("payload")) {
                Map<String, Object> payload = new HashMap<>();
                JsonNode payloadNode = jsonNode.get("payload");
                payloadNode.fieldNames().forEachRemaining(fieldName -> {
                    JsonNode fieldValue = payloadNode.get(fieldName);
                    if (fieldValue.isTextual()) {
                        payload.put(fieldName, fieldValue.asText());
                    } else if (fieldValue.isNumber()) {
                        payload.put(fieldName, fieldValue.asDouble());
                    } else if (fieldValue.isBoolean()) {
                        payload.put(fieldName, fieldValue.asBoolean());
                    }
                });
                event.setPayload(payload);
            }
            
            // Timestamps
            if (jsonNode.has("created_at")) {
                String createdAtStr = jsonNode.get("created_at").asText();
                event.setCreatedAt(new Date());
            }
            if (jsonNode.has("processed_at")) {
                String processedAtStr = jsonNode.get("processed_at").asText();
                event.setProcessedAt(new Date());
            }
            
            return event;
            
        } catch (Exception e) {
            LOG.error("Error deserializing event: " + jsonString, e);
            // Return a default event to avoid pipeline failure
            return new OutboxEvent("error", "unknown", "unknown", "unknown", new HashMap<>());
        }
    }
}
