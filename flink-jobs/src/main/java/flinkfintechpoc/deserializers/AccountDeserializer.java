package flinkfintechpoc.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import flinkfintechpoc.models.Account;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deserializer for Account objects from JSON strings
 */
public class AccountDeserializer implements MapFunction<String, Account> {
    
    private static final Logger LOG = LoggerFactory.getLogger(AccountDeserializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public Account map(String value) throws Exception {
        try {
            return objectMapper.readValue(value, Account.class);
        } catch (Exception e) {
            LOG.error("Failed to deserialize Account: {}", value, e);
            // Return a default account to prevent pipeline failure
            return new Account("error", "error", "ERROR-000", "CHECKING");
        }
    }
}
