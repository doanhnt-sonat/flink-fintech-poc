package flinkfintechpoc.deserializers;

import flinkfintechpoc.models.Customer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Customer Deserializer - Converts JSON strings to Customer objects
 */
public class CustomerDeserializer implements MapFunction<String, Customer> {
    
    private static final Logger LOG = LoggerFactory.getLogger(CustomerDeserializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public Customer map(String value) throws Exception {
        try {
            return objectMapper.readValue(value, Customer.class);
        } catch (Exception e) {
            LOG.error("Error deserializing Customer: {}", value, e);
            throw new RuntimeException("Failed to deserialize Customer", e);
        }
    }
}