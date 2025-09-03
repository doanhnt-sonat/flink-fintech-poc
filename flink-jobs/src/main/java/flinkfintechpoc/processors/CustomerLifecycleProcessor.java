package flinkfintechpoc.processors;

import flinkfintechpoc.models.CustomerSession;
import flinkfintechpoc.models.EnrichedTransaction;
import flinkfintechpoc.models.CustomerLifecycleMetrics;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Customer Lifecycle Processor - Step 2 of Cascade Pattern
 * Analyzes customer lifecycle based on EnrichedTransaction + CustomerSession data
 * Input: EnrichedTransaction + CustomerSession (broadcast)
 * Output: CustomerLifecycleMetrics
 */
public class CustomerLifecycleProcessor extends BroadcastProcessFunction<EnrichedTransaction, CustomerSession, CustomerLifecycleMetrics> {
    
    private static final Logger LOG = LoggerFactory.getLogger(CustomerLifecycleProcessor.class);
    
    // Broadcast state descriptor for customer session reference data
    public static final MapStateDescriptor<String, CustomerSession> SESSION_STATE_DESCRIPTOR = 
        new MapStateDescriptor<>(
            "session-broadcast-state",
            TypeInformation.of(new TypeHint<String>() {}),
            TypeInformation.of(new TypeHint<CustomerSession>() {})
        );
    
    // State to track customer lifecycle based on enriched transactions
    private ValueState<Map<String, Object>> customerProfile;
    private ValueState<BigDecimal> totalTransactionAmount;
    private ValueState<Integer> transactionCount;
    private ValueState<Date> firstTransactionTime;
    private ValueState<Date> lastTransactionTime;
    private ValueState<String> currentTier;
    private ValueState<String> currentKycStatus;
    private ValueState<Map<String, Object>> sessionMetrics;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // Initialize state descriptors for customer lifecycle tracking
        ValueStateDescriptor<Map<String, Object>> profileDescriptor = new ValueStateDescriptor<>(
            "customer-profile",
            TypeInformation.of(new TypeHint<Map<String, Object>>() {})
        );
        customerProfile = getRuntimeContext().getState(profileDescriptor);
        
        ValueStateDescriptor<BigDecimal> amountDescriptor = new ValueStateDescriptor<>(
            "total-transaction-amount",
            BigDecimal.class
        );
        totalTransactionAmount = getRuntimeContext().getState(amountDescriptor);
        
        ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>(
            "transaction-count",
            Integer.class
        );
        transactionCount = getRuntimeContext().getState(countDescriptor);
        
        ValueStateDescriptor<Date> firstTimeDescriptor = new ValueStateDescriptor<>(
            "first-transaction-time",
            TypeInformation.of(new TypeHint<Date>() {})
        );
        firstTransactionTime = getRuntimeContext().getState(firstTimeDescriptor);
        
        ValueStateDescriptor<Date> lastTimeDescriptor = new ValueStateDescriptor<>(
            "last-transaction-time",
            TypeInformation.of(new TypeHint<Date>() {})
        );
        lastTransactionTime = getRuntimeContext().getState(lastTimeDescriptor);
        
        ValueStateDescriptor<String> tierDescriptor = new ValueStateDescriptor<>(
            "current-tier",
            String.class
        );
        currentTier = getRuntimeContext().getState(tierDescriptor);
        
        ValueStateDescriptor<String> kycDescriptor = new ValueStateDescriptor<>(
            "current-kyc-status",
            String.class
        );
        currentKycStatus = getRuntimeContext().getState(kycDescriptor);
        
        ValueStateDescriptor<Map<String, Object>> sessionDescriptor = new ValueStateDescriptor<>(
            "session-metrics",
            TypeInformation.of(new TypeHint<Map<String, Object>>() {})
        );
        sessionMetrics = getRuntimeContext().getState(sessionDescriptor);
    }
    
    @Override
    public void processElement(EnrichedTransaction enrichedTransaction, ReadOnlyContext ctx, Collector<CustomerLifecycleMetrics> out) throws Exception {
        String customerId = enrichedTransaction.getCustomerId();
        Date currentTime = new Date();
        
        // Get session info from broadcast state
        ReadOnlyBroadcastState<String, CustomerSession> sessionState = ctx.getBroadcastState(SESSION_STATE_DESCRIPTOR);
        CustomerSession session = sessionState.get(customerId);
        
        // Initialize state if first time
        initializeStateIfNeeded(customerId, currentTime);
        
        // Update transaction-based metrics
        updateTransactionMetrics(enrichedTransaction, currentTime);
        
        // Update session-based metrics if session exists
        if (session != null) {
            updateSessionMetrics(session, currentTime);
        }
        
        // Analyze customer lifecycle based on enriched transaction and session data
        CustomerLifecycleMetrics metrics = analyzeCustomerLifecycle(enrichedTransaction, session, currentTime);
        
        if (metrics != null) {
            out.collect(metrics);
            LOG.info("Customer lifecycle event: {} for customer {} (tier: {}, kyc: {}, session: {})", 
                    metrics.getEventType(), customerId, metrics.getCurrentTier(), 
                    metrics.getCurrentKycStatus(), session != null ? session.getChannel() : "none");
        }
    }
    
    @Override
    public void processBroadcastElement(CustomerSession session, Context ctx, Collector<CustomerLifecycleMetrics> out) throws Exception {
        // Update broadcast state with customer session reference data
        BroadcastState<String, CustomerSession> sessionState = ctx.getBroadcastState(SESSION_STATE_DESCRIPTOR);
        sessionState.put(session.getCustomerId(), session);
        
        LOG.info("Updated session broadcast state: {} - {} ({})", 
                session.getCustomerId(), session.getChannel(), session.getDeviceType());
    }
    
    private void initializeStateIfNeeded(String customerId, Date currentTime) throws Exception {
        if (customerProfile.value() == null) {
            Map<String, Object> profile = new HashMap<>();
            profile.put("customerId", customerId);
            profile.put("firstTransaction", currentTime);
            profile.put("totalTransactions", 0);
            profile.put("totalAmount", BigDecimal.ZERO);
            customerProfile.update(profile);
        }
        if (totalTransactionAmount.value() == null) {
            totalTransactionAmount.update(BigDecimal.ZERO);
        }
        if (transactionCount.value() == null) {
            transactionCount.update(0);
        }
        if (firstTransactionTime.value() == null) {
            firstTransactionTime.update(currentTime);
        }
        if (lastTransactionTime.value() == null) {
            lastTransactionTime.update(currentTime);
        }
        if (currentTier.value() == null) {
            currentTier.update("basic");
        }
        if (currentKycStatus.value() == null) {
            currentKycStatus.update("pending");
        }
        if (sessionMetrics.value() == null) {
            Map<String, Object> sessionData = new HashMap<>();
            sessionData.put("totalSessions", 0);
            sessionData.put("totalActions", 0);
            sessionData.put("preferredChannel", "unknown");
            sessionData.put("preferredDevice", "unknown");
            sessionMetrics.update(sessionData);
        }
    }
    
    private void updateTransactionMetrics(EnrichedTransaction enrichedTransaction, Date currentTime) throws Exception {
        BigDecimal currentTotal = totalTransactionAmount.value();
        Integer currentCount = transactionCount.value();
        
        // Update transaction metrics
        totalTransactionAmount.update(currentTotal.add(enrichedTransaction.getAmount()));
        transactionCount.update(currentCount + 1);
        lastTransactionTime.update(currentTime);
        
        // Update customer profile
        Map<String, Object> profile = customerProfile.value();
        profile.put("totalTransactions", currentCount + 1);
        profile.put("totalAmount", currentTotal.add(enrichedTransaction.getAmount()));
        profile.put("lastTransaction", currentTime);
        customerProfile.update(profile);
    }
    
    private void updateSessionMetrics(CustomerSession session, Date currentTime) throws Exception {
        Map<String, Object> sessionData = sessionMetrics.value();
        
        // Update session metrics
        Integer totalSessions = (Integer) sessionData.getOrDefault("totalSessions", 0);
        Integer totalActions = (Integer) sessionData.getOrDefault("totalActions", 0);
        
        sessionData.put("totalSessions", totalSessions + 1);
        sessionData.put("totalActions", totalActions + session.getActionsCount());
        sessionData.put("preferredChannel", session.getChannel());
        sessionData.put("preferredDevice", session.getDeviceType());
        sessionData.put("lastSessionUpdate", currentTime);
        
        sessionMetrics.update(sessionData);
    }
    
    private CustomerLifecycleMetrics analyzeCustomerLifecycle(EnrichedTransaction enrichedTransaction, CustomerSession session, Date currentTime) throws Exception {
        BigDecimal totalAmount = totalTransactionAmount.value();
        Integer count = transactionCount.value();
        String currentTierValue = currentTier.value();
        String currentKycValue = currentKycStatus.value();
        Map<String, Object> sessionData = sessionMetrics.value();
        
        // Determine lifecycle events based on transaction patterns and session data
        String eventType = "TRANSACTION_UPDATE";
        boolean isUpgrade = false;
        boolean isDowngrade = false;
        boolean kycCompleted = false;
        boolean sessionInsight = false;
        
        // Check for tier changes based on transaction volume
        String newTier = determineTierFromTransactions(totalAmount, count);
        if (!newTier.equals(currentTierValue)) {
            if (isTierUpgrade(currentTierValue, newTier)) {
                eventType = "TIER_UPGRADE";
                isUpgrade = true;
            } else {
                eventType = "TIER_DOWNGRADE";
                isDowngrade = true;
            }
            currentTier.update(newTier);
        }
        
        // Check for KYC completion based on transaction patterns
        if ("pending".equals(currentKycValue) && count > 10 && totalAmount.compareTo(new BigDecimal("10000")) > 0) {
            eventType = "KYC_COMPLETED";
            kycCompleted = true;
            currentKycStatus.update("completed");
        }
        
        // Check for customer activation based on transaction activity
        if (count == 1) {
            eventType = "FIRST_TRANSACTION";
        } else if (count > 50) {
            eventType = "HIGH_ACTIVITY_CUSTOMER";
        }
        
        // Check for session-based insights
        if (session != null) {
            sessionInsight = true;
            
            // Check for high engagement based on session data
            if (session.getActionsCount() > 20) {
                eventType = "HIGH_ENGAGEMENT_SESSION";
            }
            
            // Check for multi-channel usage
            String preferredChannel = (String) sessionData.get("preferredChannel");
            if (!preferredChannel.equals(session.getChannel())) {
                eventType = "MULTI_CHANNEL_CUSTOMER";
            }
        }
        
        // Create lifecycle metrics with session insights
        CustomerLifecycleMetrics metrics = new CustomerLifecycleMetrics(
            enrichedTransaction.getCustomerId(),
            eventType,
            newTier,
            currentKycStatus.value(),
            currentTime,
            count,
            isUpgrade,
            isDowngrade,
            kycCompleted,
            enrichedTransaction.getCustomerRiskScore() != null ? enrichedTransaction.getCustomerRiskScore().doubleValue() : 0.0
        );
        
        // Add session insights as additional metadata (if needed)
        if (sessionInsight && session != null) {
            // Log session insights for now - could be extended to store in additional fields
            LOG.info("Session insight for customer {}: channel={}, device={}, actions={}", 
                    enrichedTransaction.getCustomerId(), session.getChannel(), 
                    session.getDeviceType(), session.getActionsCount());
        }
        
        return metrics;
    }
    
    private String determineTierFromTransactions(BigDecimal totalAmount, Integer count) {
        if (totalAmount.compareTo(new BigDecimal("100000")) > 0 && count > 100) {
            return "vip";
        } else if (totalAmount.compareTo(new BigDecimal("50000")) > 0 && count > 50) {
            return "premium";
        } else if (totalAmount.compareTo(new BigDecimal("10000")) > 0 && count > 20) {
            return "standard";
        } else {
            return "basic";
        }
    }
    
    private boolean isTierUpgrade(String oldTier, String newTier) {
        int oldTierLevel = getTierLevel(oldTier);
        int newTierLevel = getTierLevel(newTier);
        return newTierLevel > oldTierLevel;
    }
    
    private int getTierLevel(String tier) {
        switch (tier.toLowerCase()) {
            case "basic": return 1;
            case "standard": return 2;
            case "premium": return 3;
            case "vip": return 4;
            default: return 0;
        }
    }
}
