package flinkfintechpoc.processors;

import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.TimePatternMetrics;
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
 * Processor for time-based pattern metrics
 */
public class TimePatternProcessor extends ProcessWindowFunction<Transaction, TimePatternMetrics, Integer, TimeWindow> {
    
    private static final Logger LOG = LoggerFactory.getLogger(TimePatternProcessor.class);
    
    @Override
    public void process(Integer hourOfDay, 
                       Context context, 
                       Iterable<Transaction> transactions, 
                       Collector<TimePatternMetrics> collector) throws Exception {
        
        try {
            TimePatternMetrics metrics = new TimePatternMetrics(
                hourOfDay,
                new Date(context.window().getStart()),
                new Date(context.window().getEnd())
            );
            
            long transactionCount = 0;
            BigDecimal totalAmount = BigDecimal.ZERO;
            double totalRiskScore = 0.0;
            int fraudCount = 0;
            Map<String, Object> timePatternDetails = new HashMap<>();
            
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
            
            // Set time pattern details
            timePatternDetails.put("hour_of_day", hourOfDay);
            timePatternDetails.put("window_start", context.window().getStart());
            timePatternDetails.put("window_end", context.window().getEnd());
            timePatternDetails.put("fraud_count", fraudCount);
            timePatternDetails.put("legitimate_count", transactionCount - fraudCount);
            
            // Add business hours classification
            boolean isBusinessHours = hourOfDay >= 9 && hourOfDay <= 17;
            timePatternDetails.put("is_business_hours", isBusinessHours);
            
            metrics.setTimePatternDetails(timePatternDetails);
            
            LOG.debug("Generated time pattern metrics for hour {}: {} transactions, total amount: {}", 
                    hourOfDay, transactionCount, totalAmount);
            
            collector.collect(metrics);
            
        } catch (Exception e) {
            LOG.error("Error processing time pattern metrics for hour: " + hourOfDay, e);
            // Emit default metrics to avoid pipeline failure
            TimePatternMetrics defaultMetrics = new TimePatternMetrics(
                hourOfDay,
                new Date(context.window().getStart()),
                new Date(context.window().getEnd())
            );
            collector.collect(defaultMetrics);
        }
    }
}
