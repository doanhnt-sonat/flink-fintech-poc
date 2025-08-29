package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Risk analytics for transactions and customers
 */
public class RiskAnalytics {
    private String customerId;
    private Date analysisDate;
    private double overallRiskScore;
    private String riskLevel;
    private List<String> riskFactors;
    private Map<String, Double> riskContributors;
    private BigDecimal totalExposure;
    private int highRiskTransactions;
    private double fraudProbability;
    private String recommendation;
    private Map<String, Object> riskMetrics;
    
    // Constructors
    public RiskAnalytics() {}
    
    public RiskAnalytics(String customerId, Date analysisDate) {
        this.customerId = customerId;
        this.analysisDate = analysisDate;
        this.overallRiskScore = 0.0;
        this.riskLevel = "low";
        this.totalExposure = BigDecimal.ZERO;
        this.highRiskTransactions = 0;
        this.fraudProbability = 0.0;
        this.recommendation = "monitor";
    }
    
    // Getters and Setters
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public Date getAnalysisDate() { return analysisDate; }
    public void setAnalysisDate(Date analysisDate) { this.analysisDate = analysisDate; }
    
    public double getOverallRiskScore() { return overallRiskScore; }
    public void setOverallRiskScore(double overallRiskScore) { this.overallRiskScore = overallRiskScore; }
    
    public String getRiskLevel() { return riskLevel; }
    public void setRiskLevel(String riskLevel) { this.riskLevel = riskLevel; }
    
    public List<String> getRiskFactors() { return riskFactors; }
    public void setRiskFactors(List<String> riskFactors) { this.riskFactors = riskFactors; }
    
    public Map<String, Double> getRiskContributors() { return riskContributors; }
    public void setRiskContributors(Map<String, Double> riskContributors) { this.riskContributors = riskContributors; }
    
    public BigDecimal getTotalExposure() { return totalExposure; }
    public void setTotalExposure(BigDecimal totalExposure) { this.totalExposure = totalExposure; }
    
    public int getHighRiskTransactions() { return highRiskTransactions; }
    public void setHighRiskTransactions(int highRiskTransactions) { this.highRiskTransactions = highRiskTransactions; }
    
    public double getFraudProbability() { return fraudProbability; }
    public void setFraudProbability(double fraudProbability) { this.fraudProbability = fraudProbability; }
    
    public String getRecommendation() { return recommendation; }
    public void setRecommendation(String recommendation) { this.recommendation = recommendation; }
    
    public Map<String, Object> getRiskMetrics() { return riskMetrics; }
    public void setRiskMetrics(Map<String, Object> riskMetrics) { this.riskMetrics = riskMetrics; }
    
    @Override
    public String toString() {
        return "RiskAnalytics{" +
                "customerId='" + customerId + '\'' +
                ", analysisDate=" + analysisDate +
                ", overallRiskScore=" + overallRiskScore +
                ", riskLevel='" + riskLevel + '\'' +
                ", totalExposure=" + totalExposure +
                ", fraudProbability=" + fraudProbability +
                '}';
    }
}
