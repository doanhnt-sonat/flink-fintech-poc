package flinkfintechpoc.processors;

import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.TransactionMetrics;
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
 * Processor for calculating transaction metrics within time windows
 */
public class TransactionMetricsProcessor extends ProcessWindowFunction<Transaction, TransactionMetrics, String, TimeWindow> {
    
    private static final Logger LOG = LoggerFactory.getLogger(TransactionMetricsProcessor.class);
    
    @Override
    public void process(String customerId, 
                       Context context, 
                       Iterable<Transaction> transactions, 
                       Collector<TransactionMetrics> collector) throws Exception {
        
        try {
            TransactionMetrics metrics = new TransactionMetrics(
                customerId,
                new Date(context.window().getStart()),
                new Date(context.window().getEnd())
            );
            
            BigDecimal totalAmount = BigDecimal.ZERO;
            BigDecimal totalCredits = BigDecimal.ZERO;
            BigDecimal totalDebits = BigDecimal.ZERO;
            BigDecimal largestTransaction = BigDecimal.ZERO;
            BigDecimal smallestTransaction = BigDecimal.ZERO;
            double totalRiskScore = 0.0;
            int fraudCount = 0;
            
            Map<String, Integer> transactionTypeCounts = new HashMap<>();
            
            for (Transaction transaction : transactions) {
                BigDecimal amount = transaction.getAmount();
                
                // Update totals
                totalAmount = totalAmount.add(amount);
                totalRiskScore += transaction.getRiskScore();
                
                // Categorize as credit or debit
                if (isCreditTransaction(transaction)) {
                    totalCredits = totalCredits.add(amount);
                } else {
                    totalDebits = totalDebits.add(amount);
                }
                
                // Track largest and smallest transactions
                if (largestTransaction.compareTo(amount) < 0) {
                    largestTransaction = amount;
                }
                if (smallestTransaction.compareTo(BigDecimal.ZERO) == 0 || 
                    smallestTransaction.compareTo(amount) > 0) {
                    smallestTransaction = amount;
                }
                
                // Count transaction types
                String transactionType = transaction.getTransactionType();
                transactionTypeCounts.put(transactionType, 
                    transactionTypeCounts.getOrDefault(transactionType, 0) + 1);
                
                // Count fraud transactions
                if (transaction.getRiskScore() > 80.0) {
                    fraudCount++;
                }
            }
            
            // Set metrics
            metrics.setTransactionCount(transactionTypeCounts.values().stream().mapToInt(Integer::intValue).sum());
            metrics.setTotalAmount(totalAmount);
            metrics.setTotalCredits(totalCredits);
            metrics.setTotalDebits(totalDebits);
            metrics.setLargestTransaction(largestTransaction);
            metrics.setSmallestTransaction(smallestTransaction);
            metrics.setFraudCount(fraudCount);
            
            // Calculate average amount
            if (metrics.getTransactionCount() > 0) {
                metrics.setAverageAmount(totalAmount.divide(
                    BigDecimal.valueOf(metrics.getTransactionCount()), 2, RoundingMode.HALF_UP));
                metrics.setAverageRiskScore(totalRiskScore / metrics.getTransactionCount());
            }
            
            // Find most common transaction type
            String mostCommonType = transactionTypeCounts.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse("unknown");
            metrics.setMostCommonType(mostCommonType);
            
            // Set account ID if available
            if (transactions.iterator().hasNext()) {
                Transaction firstTransaction = transactions.iterator().next();
                if (firstTransaction.getFromAccountId() != null) {
                    metrics.setAccountId(firstTransaction.getFromAccountId());
                }
            }
            
            LOG.info("Generated metrics for customer {}: {} transactions, total amount: {}", 
                    customerId, metrics.getTransactionCount(), metrics.getTotalAmount());
            
            collector.collect(metrics);
            
        } catch (Exception e) {
            LOG.error("Error processing transaction metrics for customer: " + customerId, e);
            // Emit default metrics to avoid pipeline failure
            TransactionMetrics defaultMetrics = new TransactionMetrics(
                customerId,
                new Date(context.window().getStart()),
                new Date(context.window().getEnd())
            );
            collector.collect(defaultMetrics);
        }
    }
    
    private boolean isCreditTransaction(Transaction transaction) {
        // Determine if transaction is a credit based on type
        String type = transaction.getTransactionType();
        return "deposit".equals(type) || 
               "refund".equals(type) || 
               "interest_payment".equals(type) ||
               "loan_disbursement".equals(type);
    }
}
