package flinkfintechpoc.processors;

import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.Account;
import flinkfintechpoc.models.FraudDetectionResult;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.api.java.tuple.Tuple2;

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
public class CustomerFraudDetectionProcessor extends KeyedBroadcastProcessFunction<String, Transaction, Account, FraudDetectionResult> {
    
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
    private ListState<Tuple2<Long, BigDecimal>> rolling24hTransactionEntries;
    private ListState<Tuple2<Long, String>> rollingLocationEntries24h;
    private ListState<Tuple2<Long, String>> rollingDeviceEntries24h;
    private ValueState<Date> lastTransactionTime;
    
    // Fraud detection thresholds
    private static final BigDecimal HIGH_AMOUNT_THRESHOLD = new BigDecimal("10000.00");
    private static final BigDecimal DAILY_LIMIT_THRESHOLD = new BigDecimal("50000.00");
    private static final int MAX_TRANSACTIONS_PER_HOUR = 10;
    private static final int MAX_LOCATIONS_PER_DAY = 5;
    private static final int MAX_DEVICES_PER_DAY = 3;
    private static final long RAPID_TRANSACTION_THRESHOLD = 60000; // 1 minute
    
    @Override
    public void open(OpenContext openContext) throws Exception {
        // Initialize state descriptors for complex fraud detection
        ValueStateDescriptor<List<Transaction>> transactionsDescriptor = new ValueStateDescriptor<>(
            "recent-transactions",
            TypeInformation.of(new TypeHint<List<Transaction>>() {})
        );
        recentTransactions = getRuntimeContext().getState(transactionsDescriptor);
        
        
        ListStateDescriptor<Tuple2<Long, BigDecimal>> rolling24hDesc = new ListStateDescriptor<>(
            "rolling-24h-entries",
            TypeInformation.of(new TypeHint<Tuple2<Long, BigDecimal>>() {})
        );
        rolling24hTransactionEntries = getRuntimeContext().getListState(rolling24hDesc);
        ListStateDescriptor<Tuple2<Long, String>> rollingLocDesc = new ListStateDescriptor<>(
            "rolling-24h-locations",
            TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {})
        );
        rollingLocationEntries24h = getRuntimeContext().getListState(rollingLocDesc);
        ListStateDescriptor<Tuple2<Long, String>> rollingDevDesc = new ListStateDescriptor<>(
            "rolling-24h-devices",
            TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {})
        );
        rollingDeviceEntries24h = getRuntimeContext().getListState(rollingDevDesc);
        
        ValueStateDescriptor<Date> timeDescriptor = new ValueStateDescriptor<>(
            "last-transaction-time",
            TypeInformation.of(new TypeHint<Date>() {})
        );
        lastTransactionTime = getRuntimeContext().getState(timeDescriptor);
        
        
    }
    
    @Override
    public void processElement(Transaction transaction, ReadOnlyContext ctx, Collector<FraudDetectionResult> out) throws Exception {
        String customerId = transaction.getCustomerId();
        Date currentTime = transaction.getCreatedAt();
        
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
        // Update rolling 24h metrics (sum/count)
        updateRolling24hMetrics(currentTime, transaction.getAmount());
        
        // Perform comprehensive fraud detection with account data
        FraudDetectionResult result = performFraudDetection(transaction, account, currentTime);
        
        if (result != null && result.isFraudulent()) {
            out.collect(result);
            LOG.warn("Fraud detected for customer {}: {} - Risk Score: {}", 
                    customerId, result.getFraudTypes(), result.getRiskScore());
        }
        
        // Removed customer profile updates
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
        
        if (lastTransactionTime.value() == null) {
            lastTransactionTime.update(currentTime);
        }
        
    }
    
    private void updateTransactionHistory(Transaction transaction, Date currentTime) throws Exception {
        List<Transaction> transactions = recentTransactions.value();
        
        // Add current transaction
        transactions.add(transaction);
        
        // Update rolling 24h location/device entries (for true 24h counts)
        if (transaction.getTransactionLocation() != null) {
            String country = (String) transaction.getTransactionLocation().get("country");
            if (country != null) {
                appendRollingValue(currentTime, country, rollingLocationEntries24h);
            }
        }
        if (transaction.getDeviceFingerprint() != null) {
            appendRollingValue(currentTime, transaction.getDeviceFingerprint(), rollingDeviceEntries24h);
        }
        
        // Clean up old transactions (keep last 100)
        if (transactions.size() > 100) {
            transactions = transactions.subList(transactions.size() - 100, transactions.size());
        }
        
        // Update state
        recentTransactions.update(transactions);
    }
    
    private FraudDetectionResult performFraudDetection(Transaction transaction, Account account, Date currentTime) throws Exception {
        List<Transaction> transactions = recentTransactions.value();
        // legacy counters removed; use rolling lists instead (computed later)
        Tuple2<BigDecimal, Integer> amountStats = computeRollingAmountStats(currentTime);
        BigDecimal totalAmount = amountStats.f0;
        Integer transactionCount = amountStats.f1;
        Date lastTime = lastTransactionTime.value();
        
        // 24h counters computed from rolling entries
        if (totalAmount == null) totalAmount = BigDecimal.ZERO;
        if (transactionCount == null) transactionCount = 0;
        
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
        
        // 4. Multiple locations in short time (24h unique count)
        Map<String, Integer> locations = computeRollingCounts(currentTime, rollingLocationEntries24h);
        if (locations.size() > MAX_LOCATIONS_PER_DAY) {
            fraudTypes.add("MULTIPLE_LOCATIONS");
            riskScore += 20.0;
        }
        
        // 5. Multiple devices (24h unique count)
        Map<String, Integer> devices = computeRollingCounts(currentTime, rollingDeviceEntries24h);
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
        
        // 8. High risk customer (removed profile-based adjustment)
        
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
        
        // Update state (last transaction time only)
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
    
    

    private void updateRolling24hMetrics(Date currentTime, BigDecimal amount) throws Exception {
        // no-op: rolling stats computed on demand; keep entries trimmed

        long nowTs = currentTime.getTime();
        long cutoff = nowTs - 24L * 60L * 60L * 1000L;

        // Load existing entries
        List<Tuple2<Long, BigDecimal>> entries = new ArrayList<>();
        for (Tuple2<Long, BigDecimal> e : rolling24hTransactionEntries.get()) {
            entries.add(e);
        }

        // Append current
        entries.add(Tuple2.of(nowTs, amount));

        // Prune and recompute
        // compute lazily in computeRollingAmountStats
        List<Tuple2<Long, BigDecimal>> pruned = new ArrayList<>();
        for (Tuple2<Long, BigDecimal> e : entries) {
            if (e.f0 >= cutoff) {
                pruned.add(e);
            }
        }

        // After pruning, compute stats lazily when needed
        rolling24hTransactionEntries.update(pruned);
    }

    private Tuple2<BigDecimal, Integer> computeRollingAmountStats(Date currentTime) throws Exception {
        long nowTs = currentTime.getTime();
        long cutoff = nowTs - 24L * 60L * 60L * 1000L;
        BigDecimal sum = BigDecimal.ZERO;
        int count = 0;
        for (Tuple2<Long, BigDecimal> e : rolling24hTransactionEntries.get()) {
            if (e.f0 >= cutoff) {
                sum = sum.add(e.f1);
                count++;
            }
        }
        return Tuple2.of(sum, count);
    }

    private void appendRollingValue(Date currentTime, String value, ListState<Tuple2<Long, String>> target) throws Exception {
        long nowTs = currentTime.getTime();
        long cutoff = nowTs - 24L * 60L * 60L * 1000L;

        List<Tuple2<Long, String>> entries = new ArrayList<>();
        for (Tuple2<Long, String> e : target.get()) entries.add(e);
        entries.add(Tuple2.of(nowTs, value));

        List<Tuple2<Long, String>> pruned = new ArrayList<>();
        for (Tuple2<Long, String> e : entries) if (e.f0 >= cutoff) pruned.add(e);
        target.update(pruned);
    }

    private Map<String, Integer> computeRollingCounts(Date currentTime, ListState<Tuple2<Long, String>> source) throws Exception {
        long nowTs = currentTime.getTime();
        long cutoff = nowTs - 24L * 60L * 60L * 1000L;
        Map<String, Integer> counts = new HashMap<>();
        for (Tuple2<Long, String> e : source.get()) {
            if (e.f0 >= cutoff && e.f1 != null) {
                counts.put(e.f1, counts.getOrDefault(e.f1, 0) + 1);
            }
        }
        return counts;
    }
}
