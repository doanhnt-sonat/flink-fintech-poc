package flinkfintechpoc.processors;

import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.Account;
import flinkfintechpoc.models.FraudDetectionResult;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;

import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

/**
 * Customer Fraud Detection Processor - Demonstrates Complex Event Processing
 * Covers: CEP patterns, State management, Custom triggers, Stream joins simulation
 * Analyzes fraud patterns for each customer using transaction and account data
 */
public class CustomerFraudDetectionProcessor extends BroadcastProcessFunction<Transaction, Account, FraudDetectionResult> {
    
    private static final Logger LOG = LoggerFactory.getLogger(CustomerFraudDetectionProcessor.class);
    
    // Broadcast state descriptor for account reference data
    public static final MapStateDescriptor<String, Account> ACCOUNT_STATE_DESCRIPTOR = 
        new MapStateDescriptor<>(
            "account-broadcast-state",
            TypeInformation.of(new TypeHint<String>() {}),
            TypeInformation.of(new TypeHint<Account>() {})
        );
    
    // State for fraud detection patterns
    private ValueState<List<Transaction>> recentTransactions;
    private ValueState<Map<String, Integer>> locationCounts;
    private ValueState<Map<String, Integer>> deviceCounts;
    private ValueState<BigDecimal> totalAmount24h;
    private ValueState<Integer> transactionCount24h;
    private ValueState<Date> lastTransactionTime;
    private ValueState<Boolean> isHighRiskCustomer;
    private ValueState<Map<String, Object>> customerProfile;
    
    // Fraud detection thresholds
    private static final BigDecimal HIGH_AMOUNT_THRESHOLD = new BigDecimal("10000.00");
    private static final BigDecimal DAILY_LIMIT_THRESHOLD = new BigDecimal("50000.00");
    private static final int MAX_TRANSACTIONS_PER_HOUR = 10;
    private static final int MAX_LOCATIONS_PER_DAY = 5;
    private static final int MAX_DEVICES_PER_DAY = 3;
    private static final long RAPID_TRANSACTION_THRESHOLD = 60000; // 1 minute
    
    @Override
    public void open(Configuration parameters) throws Exception {
        // Initialize state descriptors for complex fraud detection
        ValueStateDescriptor<List<Transaction>> transactionsDescriptor = new ValueStateDescriptor<>(
            "recent-transactions",
            TypeInformation.of(new TypeHint<List<Transaction>>() {})
        );
        recentTransactions = getRuntimeContext().getState(transactionsDescriptor);
        
        ValueStateDescriptor<Map<String, Integer>> locationDescriptor = new ValueStateDescriptor<>(
            "location-counts",
            TypeInformation.of(new TypeHint<Map<String, Integer>>() {})
        );
        locationCounts = getRuntimeContext().getState(locationDescriptor);
        
        ValueStateDescriptor<Map<String, Integer>> deviceDescriptor = new ValueStateDescriptor<>(
            "device-counts",
            TypeInformation.of(new TypeHint<Map<String, Integer>>() {})
        );
        deviceCounts = getRuntimeContext().getState(deviceDescriptor);
        
        ValueStateDescriptor<BigDecimal> amountDescriptor = new ValueStateDescriptor<>(
            "total-amount-24h",
            BigDecimal.class
        );
        totalAmount24h = getRuntimeContext().getState(amountDescriptor);
        
        ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>(
            "transaction-count-24h",
            Integer.class
        );
        transactionCount24h = getRuntimeContext().getState(countDescriptor);
        
        ValueStateDescriptor<Date> timeDescriptor = new ValueStateDescriptor<>(
            "last-transaction-time",
            TypeInformation.of(new TypeHint<Date>() {})
        );
        lastTransactionTime = getRuntimeContext().getState(timeDescriptor);
        
        ValueStateDescriptor<Boolean> riskDescriptor = new ValueStateDescriptor<>(
            "is-high-risk-customer",
            Boolean.class
        );
        isHighRiskCustomer = getRuntimeContext().getState(riskDescriptor);
        
        ValueStateDescriptor<Map<String, Object>> profileDescriptor = new ValueStateDescriptor<>(
            "customer-profile",
            TypeInformation.of(new TypeHint<Map<String, Object>>() {})
        );
        customerProfile = getRuntimeContext().getState(profileDescriptor);
    }
    
    @Override
    public void processElement(Transaction transaction, ReadOnlyContext ctx, Collector<FraudDetectionResult> out) throws Exception {
        String customerId = transaction.getCustomerId();
        Date currentTime = new Date();
        
        // Get account info from broadcast state
        ReadOnlyBroadcastState<String, Account> accountState = ctx.getBroadcastState(ACCOUNT_STATE_DESCRIPTOR);
        Account account = accountState.get(transaction.getFromAccountId());
        
        if (account == null) {
            LOG.warn("Account not found for ID: {}", transaction.getFromAccountId());
            return;
        }
        
        // Initialize state if first time
        initializeStateIfNeeded(customerId, currentTime);
        
        // Update transaction history
        updateTransactionHistory(transaction, currentTime);
        
        // Perform comprehensive fraud detection with account data
        FraudDetectionResult result = performFraudDetection(transaction, account, currentTime);
        
        if (result != null && result.isFraudulent()) {
            out.collect(result);
            LOG.warn("Fraud detected for customer {}: {} - Risk Score: {}", 
                    customerId, result.getFraudTypes(), result.getRiskScore());
        }
        
        // Update customer profile for future analysis
        updateCustomerProfile(transaction, currentTime);
    }
    
    @Override
    public void processBroadcastElement(Account account, Context ctx, Collector<FraudDetectionResult> out) throws Exception {
        // Update broadcast state with account reference data
        BroadcastState<String, Account> accountState = ctx.getBroadcastState(ACCOUNT_STATE_DESCRIPTOR);
        accountState.put(account.getId(), account);
        
        LOG.info("Updated broadcast state with account: {} - {}", account.getId(), account.getAccountType());
    }
    
    private void initializeStateIfNeeded(String customerId, Date currentTime) throws Exception {
        if (recentTransactions.value() == null) {
            recentTransactions.update(new ArrayList<>());
        }
        if (locationCounts.value() == null) {
            locationCounts.update(new HashMap<>());
        }
        if (deviceCounts.value() == null) {
            deviceCounts.update(new HashMap<>());
        }
        if (totalAmount24h.value() == null) {
            totalAmount24h.update(BigDecimal.ZERO);
        }
        if (transactionCount24h.value() == null) {
            transactionCount24h.update(0);
        }
        if (lastTransactionTime.value() == null) {
            lastTransactionTime.update(currentTime);
        }
        if (isHighRiskCustomer.value() == null) {
            isHighRiskCustomer.update(false);
        }
        if (customerProfile.value() == null) {
            Map<String, Object> profile = new HashMap<>();
            profile.put("firstTransaction", currentTime);
            profile.put("totalTransactions", 0);
            profile.put("averageAmount", BigDecimal.ZERO);
            customerProfile.update(profile);
        }
    }
    
    private void updateTransactionHistory(Transaction transaction, Date currentTime) throws Exception {
        List<Transaction> transactions = recentTransactions.value();
        Map<String, Integer> locations = locationCounts.value();
        Map<String, Integer> devices = deviceCounts.value();
        
        // Add current transaction
        transactions.add(transaction);
        
        // Update location counts
        if (transaction.getTransactionLocation() != null) {
            String country = (String) transaction.getTransactionLocation().get("country");
            if (country != null) {
                locations.put(country, locations.getOrDefault(country, 0) + 1);
            }
        }
        
        // Update device counts
        if (transaction.getDeviceFingerprint() != null) {
            devices.put(transaction.getDeviceFingerprint(), 
                       devices.getOrDefault(transaction.getDeviceFingerprint(), 0) + 1);
        }
        
        // Clean up old transactions (keep last 100)
        if (transactions.size() > 100) {
            transactions = transactions.subList(transactions.size() - 100, transactions.size());
        }
        
        // Update state
        recentTransactions.update(transactions);
        locationCounts.update(locations);
        deviceCounts.update(devices);
    }
    
    private FraudDetectionResult performFraudDetection(Transaction transaction, Account account, Date currentTime) throws Exception {
        List<Transaction> transactions = recentTransactions.value();
        Map<String, Integer> locations = locationCounts.value();
        Map<String, Integer> devices = deviceCounts.value();
        BigDecimal totalAmount = totalAmount24h.value();
        Integer transactionCount = transactionCount24h.value();
        Date lastTime = lastTransactionTime.value();
        Boolean isHighRisk = isHighRiskCustomer.value();
        
        // Update 24h counters
        totalAmount = totalAmount.add(transaction.getAmount());
        transactionCount++;
        
        // Check for various fraud patterns
        List<String> fraudTypes = new ArrayList<>();
        double riskScore = 0.0;
        
        // 1. High amount transaction
        if (transaction.getAmount().compareTo(HIGH_AMOUNT_THRESHOLD) > 0) {
            fraudTypes.add("HIGH_AMOUNT");
            riskScore += 30.0;
        }
        
        // 2. Daily limit exceeded
        if (totalAmount.compareTo(DAILY_LIMIT_THRESHOLD) > 0) {
            fraudTypes.add("DAILY_LIMIT_EXCEEDED");
            riskScore += 40.0;
        }
        
        // 3. Too many transactions per hour
        long oneHourAgo = currentTime.getTime() - 3600000; // 1 hour
        long recentCount = transactions.stream()
            .filter(t -> t.getCreatedAt().getTime() > oneHourAgo)
            .count();
        if (recentCount > MAX_TRANSACTIONS_PER_HOUR) {
            fraudTypes.add("FREQUENT_TRANSACTIONS");
            riskScore += 25.0;
        }
        
        // 4. Multiple locations in short time
        if (locations.size() > MAX_LOCATIONS_PER_DAY) {
            fraudTypes.add("MULTIPLE_LOCATIONS");
            riskScore += 20.0;
        }
        
        // 5. Multiple devices
        if (devices.size() > MAX_DEVICES_PER_DAY) {
            fraudTypes.add("MULTIPLE_DEVICES");
            riskScore += 15.0;
        }
        
        // 6. Rapid transactions
        if (lastTime != null) {
            long timeDiff = currentTime.getTime() - lastTime.getTime();
            if (timeDiff < RAPID_TRANSACTION_THRESHOLD) {
                fraudTypes.add("RAPID_TRANSACTIONS");
                riskScore += 35.0;
            }
        }
        
        // 7. Unusual transaction patterns
        if (isUnusualPattern(transaction, transactions)) {
            fraudTypes.add("UNUSUAL_PATTERN");
            riskScore += 20.0;
        }
        
        // 8. High risk customer
        if (isHighRisk) {
            riskScore += 15.0;
        }
        
        // 9. Account-based fraud detection
        if (account.isFrozen()) {
            fraudTypes.add("FROZEN_ACCOUNT");
            riskScore += 50.0;
        }
        
        if (!account.isActive()) {
            fraudTypes.add("INACTIVE_ACCOUNT");
            riskScore += 30.0;
        }
        
        // Check if transaction amount exceeds account limits (if available)
        if (account.getMinimumBalance() != null && transaction.getAmount().compareTo(account.getMinimumBalance().multiply(new BigDecimal("10"))) > 0) {
            fraudTypes.add("EXCESSIVE_AMOUNT");
            riskScore += 25.0;
        }
        
        // Update state
        totalAmount24h.update(totalAmount);
        transactionCount24h.update(transactionCount);
        lastTransactionTime.update(currentTime);
        
        // Create fraud detection result
        if (!fraudTypes.isEmpty() && riskScore > 50.0) {
            return new FraudDetectionResult(
                transaction.getId(),
                transaction.getCustomerId(),
                fraudTypes,
                riskScore,
                currentTime,
                "FRAUD_DETECTED",
                generateFraudDetails(transaction, fraudTypes, riskScore)
            );
        }
        
        return null;
    }
    
    private boolean isUnusualPattern(Transaction current, List<Transaction> history) {
        if (history.size() < 5) return false;
        
        // Check for unusual time patterns
        long currentHour = current.getCreatedAt().toInstant().atZone(java.time.ZoneId.systemDefault()).getHour();
        
        // Check for unusual amount patterns
        BigDecimal averageAmount = history.stream()
            .map(Transaction::getAmount)
            .reduce(BigDecimal.ZERO, BigDecimal::add)
            .divide(BigDecimal.valueOf(history.size()), 2, java.math.RoundingMode.HALF_UP);
        
        return (currentHour < 6 || currentHour > 22) || 
               current.getAmount().compareTo(averageAmount.multiply(new BigDecimal("3"))) > 0;
    }
    
    private Map<String, Object> generateFraudDetails(Transaction transaction, List<String> fraudTypes, double riskScore) {
        Map<String, Object> details = new HashMap<>();
        details.put("fraudTypes", fraudTypes);
        details.put("riskScore", riskScore);
        details.put("transactionAmount", transaction.getAmount());
        details.put("transactionType", transaction.getTransactionType());
        details.put("location", transaction.getTransactionLocation());
        details.put("deviceFingerprint", transaction.getDeviceFingerprint());
        details.put("timestamp", transaction.getCreatedAt());
        return details;
    }
    
    private void updateCustomerProfile(Transaction transaction, Date currentTime) throws Exception {
        Map<String, Object> profile = customerProfile.value();
        
        int totalTransactions = (Integer) profile.get("totalTransactions") + 1;
        BigDecimal averageAmount = (BigDecimal) profile.get("averageAmount");
        
        // Update average amount
        BigDecimal newAverage = averageAmount.multiply(BigDecimal.valueOf(totalTransactions - 1))
            .add(transaction.getAmount())
            .divide(BigDecimal.valueOf(totalTransactions), 2, java.math.RoundingMode.HALF_UP);
        
        profile.put("totalTransactions", totalTransactions);
        profile.put("averageAmount", newAverage);
        profile.put("lastTransaction", currentTime);
        
        // Mark as high risk if certain conditions are met
        if (totalTransactions > 50 && newAverage.compareTo(new BigDecimal("5000")) > 0) {
            isHighRiskCustomer.update(true);
        }
        
        customerProfile.update(profile);
    }
}
