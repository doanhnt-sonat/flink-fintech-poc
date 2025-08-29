package flinkfintechpoc.processors;

import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.MerchantMetrics;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Processor for merchant transaction metrics
 */
public class MerchantMetricsProcessor extends ProcessWindowFunction<Transaction, MerchantMetrics, String, TimeWindow> {
    
    private static final Logger LOG = LoggerFactory.getLogger(MerchantMetricsProcessor.class);
    
    @Override
    public void process(String merchantId, 
                       Context context, 
                       Iterable<Transaction> transactions, 
                       Collector<MerchantMetrics> collector) throws Exception {
        
        try {
            MerchantMetrics metrics = new MerchantMetrics(
                merchantId,
                new Date(context.window().getStart()),
                new Date(context.window().getEnd())
            );
            
            long transactionCount = 0;
            BigDecimal totalAmount = BigDecimal.ZERO;
            double totalRiskScore = 0.0;
            int fraudCount = 0;
            Map<String, Object> merchantDetails = new HashMap<>();
            
            for (Transaction transaction : transactions) {
                transactionCount++;
                totalAmount = totalAmount.add(transaction.getAmount());
                totalRiskScore += transaction.getRiskScore();
                
                // Count fraud transactions
                if (transaction.getRiskScore() > 80.0) {
                    fraudCount++;
                }
                
                // Collect merchant details from first transaction
                if (transactionCount == 1) {
                    // In a real implementation, you'd enrich this with merchant data from a lookup table
                    merchantDetails.put("merchant_id", merchantId);
                    merchantDetails.put("business_type", "unknown");
                    merchantDetails.put("mcc_code", "unknown");
                }
            }
            
            // Set basic metrics
            metrics.setTransactionCount(transactionCount);
            metrics.setTotalAmount(totalAmount);
            
            // Calculate averages
            if (transactionCount > 0) {
                metrics.setAverageAmount(totalAmount.divide(
                    BigDecimal.valueOf(transactionCount), 2, RoundingMode.HALF_UP));
                metrics.setAverageRiskScore(totalRiskScore / transactionCount);
            }
            
            // Calculate fraud rate
            if (transactionCount > 0) {
                metrics.setFraudRate((double) fraudCount / transactionCount);
            }
            
            // Set merchant details
            metrics.setMerchantDetails(merchantDetails);
            
            LOG.debug("Generated merchant metrics for {}: {} transactions, total amount: {}", 
                    merchantId, transactionCount, totalAmount);
            
            collector.collect(metrics);
            
        } catch (Exception e) {
            LOG.error("Error processing merchant metrics for merchant: " + merchantId, e);
            // Emit default metrics to avoid pipeline failure
            MerchantMetrics defaultMetrics = new MerchantMetrics(
                merchantId,
                new Date(context.window().getStart()),
                new Date(context.window().getEnd())
            );
            collector.collect(defaultMetrics);
        }
    }
}
