package flinkfintechpoc.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import flinkfintechpoc.models.Merchant;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deserializer for Merchant objects from JSON strings
 */
public class MerchantDeserializer implements MapFunction<String, Merchant> {
    
    private static final Logger LOG = LoggerFactory.getLogger(MerchantDeserializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public Merchant map(String value) throws Exception {
        try {
            return objectMapper.readValue(value, Merchant.class);
        } catch (Exception e) {
            LOG.error("Failed to deserialize Merchant: {}", value, e);
            // Return a default merchant to prevent pipeline failure
            return new Merchant("error", "Unknown Merchant", "Unknown", "0000");
        }
    }
}
