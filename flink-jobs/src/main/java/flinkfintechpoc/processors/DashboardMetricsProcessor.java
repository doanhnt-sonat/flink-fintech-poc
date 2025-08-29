package flinkfintechpoc.processors;

import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.DashboardMetrics;
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
 * Processor for real-time dashboard metrics
 */
public class DashboardMetricsProcessor extends ProcessWindowFunction<Transaction, DashboardMetrics, String, TimeWindow> {
    
    private static final Logger LOG = LoggerFactory.getLogger(DashboardMetricsProcessor.class);
    
    @Override
    public void process(String transactionType, 
                       Context context, 
                       Iterable<Transaction> transactions, 
                       Collector<DashboardMetrics> collector) throws Exception {
        
        try {
            DashboardMetrics metrics = new DashboardMetrics(
                "transaction_metrics",
                transactionType,
                new Date(context.window().getStart())
            );
            
            long transactionCount = 0;
            BigDecimal totalAmount = BigDecimal.ZERO;
            double totalRiskScore = 0.0;
            int fraudCount = 0;
            
            for (Transaction transaction : transactions) {
                transactionCount++;
                totalAmount = totalAmount.add(transaction.getAmount());
                totalRiskScore += transaction.getRiskScore();
                
                // Count fraud transactions
                if (transaction.getRiskScore() > 80.0) {
                    fraudCount++;
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
            
            // Set additional metrics
            Map<String, Object> additionalMetrics = new HashMap<>();
            additionalMetrics.put("window_start", context.window().getStart());
            additionalMetrics.put("window_end", context.window().getEnd());
            additionalMetrics.put("fraud_count", fraudCount);
            additionalMetrics.put("legitimate_count", transactionCount - fraudCount);
            metrics.setAdditionalMetrics(additionalMetrics);
            
            LOG.debug("Generated dashboard metrics for {}: {} transactions, total amount: {}", 
                    transactionType, transactionCount, totalAmount);
            
            collector.collect(metrics);
            
        } catch (Exception e) {
            LOG.error("Error processing dashboard metrics for transaction type: " + transactionType, e);
            // Emit default metrics to avoid pipeline failure
            DashboardMetrics defaultMetrics = new DashboardMetrics(
                "transaction_metrics",
                transactionType,
                new Date(context.window().getStart())
            );
            collector.collect(defaultMetrics);
        }
    }
}
