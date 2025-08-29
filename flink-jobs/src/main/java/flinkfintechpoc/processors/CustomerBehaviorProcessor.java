package flinkfintechpoc.processors;

import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.CustomerBehaviorMetrics;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Processor for customer behavior analysis within time windows
 */
public class CustomerBehaviorProcessor extends ProcessWindowFunction<Transaction, CustomerBehaviorMetrics, String, TimeWindow> {
    
    private static final Logger LOG = LoggerFactory.getLogger(CustomerBehaviorProcessor.class);
    
    @Override
    public void process(String customerId, 
                       Context context, 
                       Iterable<Transaction> transactions, 
                       Collector<CustomerBehaviorMetrics> collector) throws Exception {
        
        try {
            CustomerBehaviorMetrics metrics = new CustomerBehaviorMetrics(
                customerId,
                new Date(context.window().getStart())
            );
            
            BigDecimal totalAmount = BigDecimal.ZERO;
            BigDecimal totalSpending = BigDecimal.ZERO;
            BigDecimal totalIncome = BigDecimal.ZERO;
            double totalRiskScore = 0.0;
            int transactionCount = 0;
            
            Map<String, Integer> transactionTypeCounts = new HashMap<>();
            Map<String, Integer> locationCounts = new HashMap<>();
            Map<String, Integer> deviceCounts = new HashMap<>();
            
            for (Transaction transaction : transactions) {
                BigDecimal amount = transaction.getAmount();
                totalAmount = totalAmount.add(amount);
                totalRiskScore += transaction.getRiskScore();
                transactionCount++;
                
                // Categorize as spending or income
                if (isSpendingTransaction(transaction)) {
                    totalSpending = totalSpending.add(amount);
                } else {
                    totalIncome = totalIncome.add(amount);
                }
                
                // Count transaction types
                String transactionType = transaction.getTransactionType();
                transactionTypeCounts.put(transactionType, 
                    transactionTypeCounts.getOrDefault(transactionType, 0) + 1);
                
                // Count locations
                if (transaction.getTransactionLocation() != null) {
                    String country = (String) transaction.getTransactionLocation().get("country");
                    if (country != null) {
                        locationCounts.put(country, locationCounts.getOrDefault(country, 0) + 1);
                    }
                }
                
                // Count devices
                if (transaction.getDeviceFingerprint() != null) {
                    deviceCounts.put(transaction.getDeviceFingerprint(), 
                        deviceCounts.getOrDefault(transaction.getDeviceFingerprint(), 0) + 1);
                }
            }
            
            // Set basic metrics
            metrics.setTotalBalance(totalAmount);
            metrics.setMonthlySpending(totalSpending);
            metrics.setMonthlyIncome(totalIncome);
            metrics.setTotalTransactions(transactionCount);
            
            // Calculate transaction frequency (transactions per day)
            long windowDurationDays = (context.window().getEnd() - context.window().getStart()) / (24 * 60 * 60 * 1000);
            if (windowDurationDays > 0) {
                metrics.setTransactionFrequency((double) transactionCount / windowDurationDays);
            }
            
            // Calculate average risk score
            if (transactionCount > 0) {
                metrics.setAverageTransactionAmount(totalAmount.divide(
                    BigDecimal.valueOf(transactionCount), 2, RoundingMode.HALF_UP).doubleValue());
                metrics.setRiskScore(totalRiskScore / transactionCount);
            }
            
            // Find preferred transaction types (top 3)
            List<String> preferredTypes = transactionTypeCounts.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(3)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
            metrics.setPreferredTransactionTypes(preferredTypes);
            
            // Set location patterns
            metrics.setLocationPatterns(locationCounts);
            
            // Set device patterns
            metrics.setDevicePatterns(deviceCounts);
            
            // Calculate engagement score based on transaction frequency and variety
            double engagementScore = calculateEngagementScore(transactionCount, transactionTypeCounts.size(), 
                                                           locationCounts.size(), deviceCounts.size());
            metrics.setEngagementScore(engagementScore);
            
            // Calculate lifetime value (simplified)
            BigDecimal lifetimeValue = totalIncome.subtract(totalSpending);
            metrics.setLifetimeValue(lifetimeValue);
            
            // Determine customer segment
            String segment = determineCustomerSegment(totalAmount, transactionCount, engagementScore);
            metrics.setCustomerSegment(segment);
            
            LOG.info("Generated customer behavior metrics for {}: {} transactions, segment: {}", 
                    customerId, transactionCount, segment);
            
            collector.collect(metrics);
            
        } catch (Exception e) {
            LOG.error("Error processing customer behavior for customer: " + customerId, e);
            // Emit default metrics to avoid pipeline failure
            CustomerBehaviorMetrics defaultMetrics = new CustomerBehaviorMetrics(
                customerId,
                new Date(context.window().getStart())
            );
            collector.collect(defaultMetrics);
        }
    }
    
    private boolean isSpendingTransaction(Transaction transaction) {
        String type = transaction.getTransactionType();
        return "card_payment".equals(type) || 
               "wire_transfer".equals(type) || 
               "ach_transfer".equals(type) ||
               "mobile_payment".equals(type) ||
               "atm_withdrawal".equals(type) ||
               "fee_charge".equals(type);
    }
    
    private double calculateEngagementScore(int transactionCount, int transactionTypes, 
                                          int locations, int devices) {
        // Simple scoring algorithm
        double score = 0.0;
        
        // Transaction count contribution (40%)
        if (transactionCount > 0) {
            score += Math.min(40.0, transactionCount * 2.0);
        }
        
        // Transaction variety contribution (20%)
        score += Math.min(20.0, transactionTypes * 5.0);
        
        // Location variety contribution (20%)
        score += Math.min(20.0, locations * 10.0);
        
        // Device variety contribution (20%)
        score += Math.min(20.0, devices * 10.0);
        
        return Math.min(100.0, score);
    }
    
    private String determineCustomerSegment(BigDecimal totalAmount, int transactionCount, double engagementScore) {
        if (totalAmount.compareTo(new BigDecimal("10000")) > 0 && engagementScore > 70) {
            return "premium";
        } else if (totalAmount.compareTo(new BigDecimal("5000")) > 0 && engagementScore > 50) {
            return "active";
        } else if (transactionCount > 20) {
            return "regular";
        } else {
            return "basic";
        }
    }
}
