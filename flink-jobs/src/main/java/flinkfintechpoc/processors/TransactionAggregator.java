package flinkfintechpoc.processors;

import flinkfintechpoc.models.Transaction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Transaction Aggregator - Incremental aggregation for transaction metrics
 * Handles incremental updates as events arrive, reducing memory usage
 */
public class TransactionAggregator implements AggregateFunction<Transaction, TransactionAggregator.TransactionAccumulator, TransactionAggregator.TransactionAccumulator> {
    
    private static final Logger LOG = LoggerFactory.getLogger(TransactionAggregator.class);
    
    @Override
    public TransactionAccumulator createAccumulator() {
        return new TransactionAccumulator();
    }
    
    @Override
    public TransactionAccumulator add(Transaction transaction, TransactionAccumulator accumulator) {
        BigDecimal amount = transaction.getAmount();
        long timestamp = transaction.getCreatedAt().getTime();
        
        // Update basic metrics
        accumulator.totalAmount = accumulator.totalAmount.add(amount);
        accumulator.transactionCount++;
        
        // Update min/max
        if (accumulator.minAmount == null || amount.compareTo(accumulator.minAmount) < 0) {
            accumulator.minAmount = amount;
        }
        if (accumulator.maxAmount == null || amount.compareTo(accumulator.maxAmount) > 0) {
            accumulator.maxAmount = amount;
        }
        
        // Track last transaction by timestamp
        if (timestamp > accumulator.lastTimestamp) {
            accumulator.lastTimestamp = timestamp;
            accumulator.lastTransactionAmount = amount;
            accumulator.lastTransactionType = transaction.getTransactionType();
        }
        
        // Update transaction type counts
        String transactionType = transaction.getTransactionType();
        accumulator.transactionTypeCounts.put(transactionType, 
            accumulator.transactionTypeCounts.getOrDefault(transactionType, 0) + 1);
        
        // Update location counts
        if (transaction.getTransactionLocation() != null) {
            String country = (String) transaction.getTransactionLocation().get("country");
            if (country != null) {
                accumulator.locationCounts.put(country, 
                    accumulator.locationCounts.getOrDefault(country, 0) + 1);
            }
        }
        
        // Update device counts
        if (transaction.getDeviceFingerprint() != null) {
            accumulator.deviceCounts.put(transaction.getDeviceFingerprint(), 
                accumulator.deviceCounts.getOrDefault(transaction.getDeviceFingerprint(), 0) + 1);
        }
        
        LOG.debug("Aggregated transaction {}: amount={}, total={}, count={}", 
                transaction.getId(), amount, accumulator.totalAmount, accumulator.transactionCount);
        
        return accumulator;
    }
    
    @Override
    public TransactionAccumulator getResult(TransactionAccumulator accumulator) {
        return accumulator;
    }
    
    @Override
    public TransactionAccumulator merge(TransactionAccumulator a, TransactionAccumulator b) {
        // Merge two accumulators (used for checkpointing and recovery)
        TransactionAccumulator merged = new TransactionAccumulator();
        
        // Merge basic metrics
        merged.totalAmount = a.totalAmount.add(b.totalAmount);
        merged.transactionCount = a.transactionCount + b.transactionCount;
        
        // Merge min/max
        merged.minAmount = (a.minAmount == null) ? b.minAmount : 
                          (b.minAmount == null) ? a.minAmount : 
                          (a.minAmount.compareTo(b.minAmount) < 0) ? a.minAmount : b.minAmount;
        
        merged.maxAmount = (a.maxAmount == null) ? b.maxAmount : 
                          (b.maxAmount == null) ? a.maxAmount : 
                          (a.maxAmount.compareTo(b.maxAmount) > 0) ? a.maxAmount : b.maxAmount;
        
        // Merge last transaction (keep the latest by timestamp)
        merged.lastTimestamp = Math.max(a.lastTimestamp, b.lastTimestamp);
        if (a.lastTimestamp > b.lastTimestamp) {
            merged.lastTransactionAmount = a.lastTransactionAmount;
            merged.lastTransactionType = a.lastTransactionType;
        } else {
            merged.lastTransactionAmount = b.lastTransactionAmount;
            merged.lastTransactionType = b.lastTransactionType;
        }
        
        // Merge counts
        merged.transactionTypeCounts.putAll(a.transactionTypeCounts);
        b.transactionTypeCounts.forEach((k, v) -> 
            merged.transactionTypeCounts.merge(k, v, Integer::sum));
        
        merged.locationCounts.putAll(a.locationCounts);
        b.locationCounts.forEach((k, v) -> 
            merged.locationCounts.merge(k, v, Integer::sum));
        
        merged.deviceCounts.putAll(a.deviceCounts);
        b.deviceCounts.forEach((k, v) -> 
            merged.deviceCounts.merge(k, v, Integer::sum));
        
        return merged;
    }
    
    /**
     * Accumulator class to hold aggregated state
     */
    public static class TransactionAccumulator {
        public BigDecimal totalAmount = BigDecimal.ZERO;
        public int transactionCount = 0;
        public BigDecimal minAmount = null;
        public BigDecimal maxAmount = null;
        public long lastTimestamp = 0;
        public BigDecimal lastTransactionAmount = BigDecimal.ZERO;
        public String lastTransactionType = "unknown";
        public Map<String, Integer> transactionTypeCounts = new HashMap<>();
        public Map<String, Integer> locationCounts = new HashMap<>();
        public Map<String, Integer> deviceCounts = new HashMap<>();
    }
}
