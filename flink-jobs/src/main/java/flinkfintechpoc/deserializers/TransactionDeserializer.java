package flinkfintechpoc.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import flinkfintechpoc.models.Transaction;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * Deserializer for Transaction objects from Kafka JSON messages
 */
public class TransactionDeserializer implements MapFunction<String, Transaction> {
    
    private static final Logger LOG = LoggerFactory.getLogger(TransactionDeserializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public Transaction map(String value) throws Exception {
        try {
            return objectMapper.readValue(value, Transaction.class);
        } catch (Exception e) {
            LOG.error("Error deserializing transaction: {}", value, e);
            // Return a default transaction to avoid pipeline failure
            return new Transaction("error", "unknown", BigDecimal.ZERO, "unknown");
        }
    }
}
