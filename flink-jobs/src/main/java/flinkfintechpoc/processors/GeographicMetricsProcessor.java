package flinkfintechpoc.processors;

import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.GeographicMetrics;
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
 * Processor for geographic transaction metrics
 */
public class GeographicMetricsProcessor extends ProcessWindowFunction<Transaction, GeographicMetrics, String, TimeWindow> {
    
    private static final Logger LOG = LoggerFactory.getLogger(GeographicMetricsProcessor.class);
    
    @Override
    public void process(String country, 
                       Context context, 
                       Iterable<Transaction> transactions, 
                       Collector<GeographicMetrics> collector) throws Exception {
        
        try {
            GeographicMetrics metrics = new GeographicMetrics(
                country,
                new Date(context.window().getStart()),
                new Date(context.window().getEnd())
            );
            
            long transactionCount = 0;
            BigDecimal totalAmount = BigDecimal.ZERO;
            double totalRiskScore = 0.0;
            int fraudCount = 0;
            Map<String, Object> locationDetails = new HashMap<>();
            
            for (Transaction transaction : transactions) {
                transactionCount++;
                totalAmount = totalAmount.add(transaction.getAmount());
                totalRiskScore += transaction.getRiskScore();
                
                // Count fraud transactions
                if (transaction.getRiskScore() > 80.0) {
                    fraudCount++;
                }
                
                // Collect location details
                if (transaction.getTransactionLocation() != null) {
                    String city = (String) transaction.getTransactionLocation().get("city");
                    if (city != null) {
                        locationDetails.put("city", city);
                    }
                    
                    Double latitude = (Double) transaction.getTransactionLocation().get("latitude");
                    Double longitude = (Double) transaction.getTransactionLocation().get("longitude");
                    if (latitude != null && longitude != null) {
                        locationDetails.put("latitude", latitude);
                        locationDetails.put("longitude", longitude);
                    }
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
            
            // Set location details
            metrics.setLocationDetails(locationDetails);
            
            LOG.debug("Generated geographic metrics for {}: {} transactions, total amount: {}", 
                    country, transactionCount, totalAmount);
            
            collector.collect(metrics);
            
        } catch (Exception e) {
            LOG.error("Error processing geographic metrics for country: " + country, e);
            // Emit default metrics to avoid pipeline failure
            GeographicMetrics defaultMetrics = new GeographicMetrics(
                country,
                new Date(context.window().getStart()),
                new Date(context.window().getEnd())
            );
            collector.collect(defaultMetrics);
        }
    }
}
