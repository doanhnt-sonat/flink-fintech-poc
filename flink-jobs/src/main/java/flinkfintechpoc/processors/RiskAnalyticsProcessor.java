package flinkfintechpoc.processors;

import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.RiskAnalytics;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Processor for risk analytics within time windows
 */
public class RiskAnalyticsProcessor extends ProcessWindowFunction<Transaction, RiskAnalytics, String, TimeWindow> {
    
    private static final Logger LOG = LoggerFactory.getLogger(RiskAnalyticsProcessor.class);
    
    @Override
    public void process(String customerId, 
                       Context context, 
                       Iterable<Transaction> transactions, 
                       Collector<RiskAnalytics> collector) throws Exception {
        
        try {
            RiskAnalytics analytics = new RiskAnalytics(
                customerId,
                new Date(context.window().getStart())
            );
            
            BigDecimal totalExposure = BigDecimal.ZERO;
            double totalRiskScore = 0.0;
            int transactionCount = 0;
            int highRiskTransactions = 0;
            List<String> riskFactors = new ArrayList<>();
            Map<String, Double> riskContributors = new HashMap<>();
            
            for (Transaction transaction : transactions) {
                BigDecimal amount = transaction.getAmount();
                double riskScore = transaction.getRiskScore();
                
                totalExposure = totalExposure.add(amount);
                totalRiskScore += riskScore;
                transactionCount++;
                
                // Count high-risk transactions
                if (riskScore > 80.0) {
                    highRiskTransactions++;
                }
                
                // Analyze risk factors
                analyzeRiskFactors(transaction, riskFactors, riskContributors);
            }
            
            // Set basic metrics
            analytics.setTotalExposure(totalExposure);
            analytics.setHighRiskTransactions(highRiskTransactions);
            
            // Calculate overall risk score
            if (transactionCount > 0) {
                double overallRiskScore = totalRiskScore / transactionCount;
                analytics.setOverallRiskScore(overallRiskScore);
                
                // Determine risk level
                String riskLevel = determineRiskLevel(overallRiskScore);
                analytics.setRiskLevel(riskLevel);
                
                // Calculate fraud probability
                double fraudProbability = calculateFraudProbability(overallRiskScore, highRiskTransactions, transactionCount);
                analytics.setFraudProbability(fraudProbability);
            }
            
            // Set risk factors and contributors
            analytics.setRiskFactors(riskFactors);
            analytics.setRiskContributors(riskContributors);
            
            // Set recommendation
            String recommendation = determineRecommendation(analytics.getOverallRiskScore(), analytics.getFraudProbability());
            analytics.setRecommendation(recommendation);
            
            // Set additional risk metrics
            Map<String, Object> riskMetrics = new HashMap<>();
            riskMetrics.put("transaction_count", transactionCount);
            riskMetrics.put("average_transaction_amount", totalExposure.divide(BigDecimal.valueOf(transactionCount), 2, BigDecimal.ROUND_HALF_UP));
            riskMetrics.put("high_risk_percentage", transactionCount > 0 ? (double) highRiskTransactions / transactionCount * 100 : 0.0);
            analytics.setRiskMetrics(riskMetrics);
            
            LOG.info("Generated risk analytics for customer {}: risk level: {}, fraud probability: {:.2f}%", 
                    customerId, analytics.getRiskLevel(), analytics.getFraudProbability() * 100);
            
            collector.collect(analytics);
            
        } catch (Exception e) {
            LOG.error("Error processing risk analytics for customer: " + customerId, e);
            // Emit default analytics to avoid pipeline failure
            RiskAnalytics defaultAnalytics = new RiskAnalytics(
                customerId,
                new Date(context.window().getStart())
            );
            collector.collect(defaultAnalytics);
        }
    }
    
    private void analyzeRiskFactors(Transaction transaction, List<String> riskFactors, Map<String, Double> riskContributors) {
        double riskScore = transaction.getRiskScore();
        
        // High risk score
        if (riskScore > 80.0) {
            if (!riskFactors.contains("high_risk_score")) {
                riskFactors.add("high_risk_score");
            }
            riskContributors.put("high_risk_score", Math.max(riskContributors.getOrDefault("high_risk_score", 0.0), riskScore));
        }
        
        // Large transaction amount
        if (transaction.getAmount().compareTo(new BigDecimal("10000")) > 0) {
            if (!riskFactors.contains("large_amount")) {
                riskFactors.add("large_amount");
            }
            riskContributors.put("large_amount", Math.max(riskContributors.getOrDefault("large_amount", 0.0), riskScore));
        }
        
        // Unusual location
        if (transaction.getTransactionLocation() != null) {
            String country = (String) transaction.getTransactionLocation().get("country");
            if (country != null && !"US".equals(country)) {
                if (!riskFactors.contains("foreign_transaction")) {
                    riskFactors.add("foreign_transaction");
                }
                riskContributors.put("foreign_transaction", Math.max(riskContributors.getOrDefault("foreign_transaction", 0.0), riskScore));
            }
        }
        
        // Unusual device
        if (transaction.getDeviceFingerprint() != null && transaction.getDeviceFingerprint().contains("unknown")) {
            if (!riskFactors.contains("unknown_device")) {
                riskFactors.add("unknown_device");
            }
            riskContributors.put("unknown_device", Math.max(riskContributors.getOrDefault("unknown_device", 0.0), riskScore));
        }
        
        // High frequency (if we had access to historical data)
        // This would require state management in a real implementation
    }
    
    private String determineRiskLevel(double overallRiskScore) {
        if (overallRiskScore >= 80.0) {
            return "critical";
        } else if (overallRiskScore >= 60.0) {
            return "high";
        } else if (overallRiskScore >= 40.0) {
            return "medium";
        } else {
            return "low";
        }
    }
    
    private double calculateFraudProbability(double overallRiskScore, int highRiskTransactions, int totalTransactions) {
        // Base probability from risk score
        double baseProbability = overallRiskScore / 100.0;
        
        // Adjust based on high-risk transaction ratio
        double highRiskRatio = totalTransactions > 0 ? (double) highRiskTransactions / totalTransactions : 0.0;
        
        // Combine factors (weighted average)
        return (baseProbability * 0.7) + (highRiskRatio * 0.3);
    }
    
    private String determineRecommendation(double overallRiskScore, double fraudProbability) {
        if (overallRiskScore >= 80.0 || fraudProbability >= 0.8) {
            return "block";
        } else if (overallRiskScore >= 60.0 || fraudProbability >= 0.6) {
            return "review";
        } else if (overallRiskScore >= 40.0 || fraudProbability >= 0.4) {
            return "monitor";
        } else {
            return "allow";
        }
    }
}
