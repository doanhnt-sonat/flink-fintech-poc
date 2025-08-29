package flinkfintechpoc.processors;

import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.ComplianceMetrics;
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
 * Processor for compliance monitoring metrics
 */
public class ComplianceProcessor extends ProcessWindowFunction<Transaction, ComplianceMetrics, String, TimeWindow> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ComplianceProcessor.class);
    
    @Override
    public void process(String customerId, 
                       Context context, 
                       Iterable<Transaction> transactions, 
                       Collector<ComplianceMetrics> collector) throws Exception {
        
        try {
            ComplianceMetrics metrics = new ComplianceMetrics(
                customerId,
                new Date(context.window().getStart()),
                new Date(context.window().getEnd())
            );
            
            BigDecimal totalExposure = BigDecimal.ZERO;
            double totalRiskScore = 0.0;
            int transactionCount = 0;
            int suspiciousTransactions = 0;
            List<String> complianceFlags = new ArrayList<>();
            Map<String, Object> complianceDetails = new HashMap<>();
            
            for (Transaction transaction : transactions) {
                BigDecimal amount = transaction.getAmount();
                double riskScore = transaction.getRiskScore();
                
                totalExposure = totalExposure.add(amount);
                totalRiskScore += riskScore;
                transactionCount++;
                
                // Count suspicious transactions
                if (riskScore > 70.0) {
                    suspiciousTransactions++;
                }
                
                // Collect compliance flags
                if (transaction.getComplianceFlags() != null) {
                    for (String flag : transaction.getComplianceFlags()) {
                        if (!complianceFlags.contains(flag)) {
                            complianceFlags.add(flag);
                        }
                    }
                }
            }
            
            // Set basic metrics
            metrics.setTotalExposure(totalExposure);
            metrics.setSuspiciousTransactions(suspiciousTransactions);
            metrics.setComplianceFlags(complianceFlags);
            
            // Calculate compliance risk score
            if (transactionCount > 0) {
                double complianceRiskScore = totalRiskScore / transactionCount;
                metrics.setComplianceRiskScore(complianceRiskScore);
                
                // Determine compliance status
                String complianceStatus = determineComplianceStatus(complianceRiskScore, suspiciousTransactions, transactionCount);
                metrics.setComplianceStatus(complianceStatus);
            }
            
            // Set compliance details
            complianceDetails.put("customer_id", customerId);
            complianceDetails.put("window_start", context.window().getStart());
            complianceDetails.put("window_end", context.window().getEnd());
            complianceDetails.put("transaction_count", transactionCount);
            complianceDetails.put("total_exposure", totalExposure);
            complianceDetails.put("suspicious_transactions", suspiciousTransactions);
            complianceDetails.put("compliance_flags_count", complianceFlags.size());
            metrics.setComplianceDetails(complianceDetails);
            
            LOG.debug("Generated compliance metrics for customer {}: {} transactions, status: {}", 
                    customerId, transactionCount, metrics.getComplianceStatus());
            
            collector.collect(metrics);
            
        } catch (Exception e) {
            LOG.error("Error processing compliance metrics for customer: " + customerId, e);
            // Emit default metrics to avoid pipeline failure
            ComplianceMetrics defaultMetrics = new ComplianceMetrics(
                customerId,
                new Date(context.window().getStart()),
                new Date(context.window().getEnd())
            );
            collector.collect(defaultMetrics);
        }
    }
    
    private String determineComplianceStatus(double complianceRiskScore, int suspiciousTransactions, int totalTransactions) {
        // Determine compliance status based on risk score and suspicious transaction ratio
        double suspiciousRatio = totalTransactions > 0 ? (double) suspiciousTransactions / totalTransactions : 0.0;
        
        if (complianceRiskScore >= 80.0 || suspiciousRatio >= 0.3) {
            return "high_risk";
        } else if (complianceRiskScore >= 60.0 || suspiciousRatio >= 0.2) {
            return "medium_risk";
        } else if (complianceRiskScore >= 40.0 || suspiciousRatio >= 0.1) {
            return "low_risk";
        } else {
            return "compliant";
        }
    }
}
