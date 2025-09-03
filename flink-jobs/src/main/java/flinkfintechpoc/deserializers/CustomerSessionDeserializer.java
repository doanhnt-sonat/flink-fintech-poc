package flinkfintechpoc.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import flinkfintechpoc.models.CustomerSession;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deserializer for CustomerSession objects from Kafka JSON messages
 */
public class CustomerSessionDeserializer implements MapFunction<String, CustomerSession> {
    
    private static final Logger LOG = LoggerFactory.getLogger(CustomerSessionDeserializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public CustomerSession map(String value) throws Exception {
        try {
            return objectMapper.readValue(value, CustomerSession.class);
        } catch (Exception e) {
            LOG.error("Error deserializing customer session: {}", value, e);
            // Return a default session to avoid pipeline failure
            return new CustomerSession("error", "unknown", "unknown", "unknown");
        }
    }
}
