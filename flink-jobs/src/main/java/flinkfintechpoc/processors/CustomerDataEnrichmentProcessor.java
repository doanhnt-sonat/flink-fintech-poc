package flinkfintechpoc.processors;

import flinkfintechpoc.models.Customer;
import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.EnrichedTransaction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Customer Data Enrichment Processor - Step 1 of Cascade Pattern
 * Enriches Transaction data with Customer data
 * Input: Transaction + Customer (broadcast)
 * Output: EnrichedTransaction
 */
public class CustomerDataEnrichmentProcessor extends KeyedBroadcastProcessFunction<String, Transaction, Customer, EnrichedTransaction> {
    
    private static final Logger LOG = LoggerFactory.getLogger(CustomerDataEnrichmentProcessor.class);
    
    // Broadcast state descriptor for customer reference data
    public static final MapStateDescriptor<String, Customer> CUSTOMER_STATE_DESCRIPTOR = 
        new MapStateDescriptor<>(
            "customer-broadcast-state",
            TypeInformation.of(new TypeHint<String>() {}),
            TypeInformation.of(new TypeHint<Customer>() {})
        );
    
    @Override
    public void processElement(Transaction transaction, ReadOnlyContext ctx, Collector<EnrichedTransaction> out) throws Exception {
        String customerId = transaction.getCustomerId();
        
        // Get customer info from broadcast state
        ReadOnlyBroadcastState<String, Customer> customerState = ctx.getBroadcastState(CUSTOMER_STATE_DESCRIPTOR);
        Customer customer = customerState.get(customerId);
        
        if (customer == null) {
            LOG.warn("Customer not found for ID: {}, skipping transaction enrichment", customerId);
            // Still create enriched transaction without customer data
            EnrichedTransaction enriched = new EnrichedTransaction(transaction, null);
            out.collect(enriched);
            return;
        }
        
        // Create enriched transaction with customer data
        EnrichedTransaction enriched = new EnrichedTransaction(transaction, customer);
        out.collect(enriched);
        
        LOG.debug("Enriched transaction {} with customer data for customer {}", 
                transaction.getId(), customerId);
    }
    
    @Override
    public void processBroadcastElement(Customer customer, Context ctx, Collector<EnrichedTransaction> out) throws Exception {
        // Update broadcast state with customer reference data
        BroadcastState<String, Customer> customerState = ctx.getBroadcastState(CUSTOMER_STATE_DESCRIPTOR);
        customerState.put(customer.getId(), customer);
        
        LOG.info("Updated customer broadcast state: {} - {} ({})", 
                customer.getId(), customer.getEmail(), customer.getTier());
    }
}
