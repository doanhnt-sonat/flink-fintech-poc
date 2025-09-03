package flinkfintechpoc.processors;

import flinkfintechpoc.models.Merchant;
import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.MerchantAnalyticsMetrics;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Merchant Performance Processor - Demonstrates Broadcast State Pattern
 * Analyzes merchant performance using transaction data and merchant reference data
 * Tracks revenue, transaction volume, and performance metrics for each merchant
 */
public class MerchantPerformanceProcessor extends KeyedBroadcastProcessFunction<String, Transaction, Merchant, MerchantAnalyticsMetrics> {
    
    private static final Logger LOG = LoggerFactory.getLogger(MerchantPerformanceProcessor.class);
    
    // Broadcast state descriptor for merchant reference data
    public static final MapStateDescriptor<String, Merchant> MERCHANT_STATE_DESCRIPTOR = 
        new MapStateDescriptor<>(
            "merchant-broadcast-state",
            TypeInformation.of(new TypeHint<String>() {}),
            TypeInformation.of(new TypeHint<Merchant>() {})
        );
    
    // State for merchant analytics
    private ValueState<Integer> transactionCount;
    private ValueState<BigDecimal> totalAmount;
    private ValueState<Date> lastUpdateTime;
    
    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        
        // Initialize state descriptors
        ValueStateDescriptor<Integer> transactionCountDescriptor = 
            new ValueStateDescriptor<>("transaction-count", Integer.class);
        ValueStateDescriptor<BigDecimal> totalAmountDescriptor = 
            new ValueStateDescriptor<>("total-amount", BigDecimal.class);
        ValueStateDescriptor<Date> lastUpdateTimeDescriptor = 
            new ValueStateDescriptor<>("last-update-time", Date.class);
        
        transactionCount = getRuntimeContext().getState(transactionCountDescriptor);
        totalAmount = getRuntimeContext().getState(totalAmountDescriptor);
        lastUpdateTime = getRuntimeContext().getState(lastUpdateTimeDescriptor);
    }
    
    @Override
    public void processElement(Transaction transaction, ReadOnlyContext ctx, Collector<MerchantAnalyticsMetrics> out) throws Exception {
        // Process transaction and enrich with merchant data
        String merchantId = transaction.getMerchantId();
        if (merchantId == null) return;
        
        // Get merchant info from broadcast state
        ReadOnlyBroadcastState<String, Merchant> merchantState = ctx.getBroadcastState(MERCHANT_STATE_DESCRIPTOR);
        Merchant merchant = merchantState.get(merchantId);
        
        if (merchant == null) {
            LOG.warn("Merchant not found for ID: {}", merchantId);
            return;
        }
        
        // Update analytics state
        Integer currentCount = transactionCount.value();
        BigDecimal currentTotal = totalAmount.value();
        Date currentTime = new Date();
        
        if (currentCount == null) currentCount = 0;
        if (currentTotal == null) currentTotal = BigDecimal.ZERO;
        
        // Update state
        transactionCount.update(currentCount + 1);
        totalAmount.update(currentTotal.add(transaction.getAmount()));
        lastUpdateTime.update(currentTime);
        
        // Calculate analytics
        int totalTransactions = currentCount + 1;
        BigDecimal totalAmountValue = currentTotal.add(transaction.getAmount());
        BigDecimal averageAmount = totalAmountValue.divide(BigDecimal.valueOf(totalTransactions), 2, java.math.RoundingMode.HALF_UP);
        
        String performanceLevel = calculatePerformanceLevel(totalTransactions, totalAmountValue.doubleValue());
        String riskLevel = calculateRiskLevel(merchantId, totalTransactions, totalAmountValue.doubleValue());
        
        // Create and emit metrics
        MerchantAnalyticsMetrics metrics = new MerchantAnalyticsMetrics(
            merchantId,
            "MERCHANT_ANALYTICS",
            totalTransactions,
            totalAmountValue.doubleValue(),
            averageAmount.doubleValue(),
            currentTime,
            performanceLevel,
            riskLevel
        );
        
        out.collect(metrics);
        
        LOG.info("Processed transaction for merchant {}: {} transactions, total: ${}", 
                merchantId, totalTransactions, totalAmountValue);
    }
    
    @Override
    public void processBroadcastElement(Merchant merchant, Context ctx, Collector<MerchantAnalyticsMetrics> out) throws Exception {
        // Update broadcast state with merchant reference data
        BroadcastState<String, Merchant> merchantState = ctx.getBroadcastState(MERCHANT_STATE_DESCRIPTOR);
        merchantState.put(merchant.getId(), merchant);
        
        LOG.info("Updated broadcast state with merchant: {} - {}", merchant.getId(), merchant.getName());
    }
    

    
    private String calculatePerformanceLevel(int transactionCount, double totalAmount) {
        if (transactionCount > 1000 && totalAmount > 100000) {
            return "HIGH_PERFORMANCE";
        } else if (transactionCount > 500 && totalAmount > 50000) {
            return "MEDIUM_PERFORMANCE";
        } else if (transactionCount > 100 && totalAmount > 10000) {
            return "LOW_PERFORMANCE";
        } else {
            return "MINIMAL_ACTIVITY";
        }
    }
    
    private String calculateRiskLevel(String merchantId, int transactionCount, double totalAmount) {
        // Simple risk calculation based on transaction patterns
        double averageAmount = totalAmount / transactionCount;
        
        if (averageAmount > 10000) {
            return "HIGH_RISK";
        } else if (averageAmount > 5000) {
            return "MEDIUM_RISK";
        } else {
            return "LOW_RISK";
        }
    }
    

}
