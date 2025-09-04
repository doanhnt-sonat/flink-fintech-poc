package flinkfintechpoc.processors;

import flinkfintechpoc.models.Transaction;
import flinkfintechpoc.models.TransactionMetrics;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

/**
 * Customer Transaction Metrics Processor - Demonstrates Windowed Processing and State Management
 * Covers: Windowed aggregations, State management, Custom triggers, Event time processing
 * Analyzes transaction metrics for each customer within time windows
 */
public class CustomerTransactionMetricsProcessor extends ProcessWindowFunction<Transaction, TransactionMetrics, String, TimeWindow> {
    
    private static final Logger LOG = LoggerFactory.getLogger(CustomerTransactionMetricsProcessor.class);
    
    // Window state for transaction metrics
    private ValueState<BigDecimal> totalAmount;
    private ValueState<Integer> transactionCount;
    private ValueState<BigDecimal> minAmount;
    private ValueState<BigDecimal> maxAmount;
    private ValueState<Map<String, Integer>> transactionTypeCounts;
    private ValueState<Map<String, Integer>> locationCounts;
    private ValueState<Map<String, Integer>> deviceCounts;
    
    @Override
    public void open(OpenContext openContext) throws Exception {
        // Initialize window state descriptors
        ValueStateDescriptor<BigDecimal> totalAmountDescriptor = new ValueStateDescriptor<>(
            "total-amount",
            BigDecimal.class
        );
        totalAmount = getRuntimeContext().getState(totalAmountDescriptor);
        
        ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>(
            "transaction-count",
            Integer.class
        );
        transactionCount = getRuntimeContext().getState(countDescriptor);
        
        ValueStateDescriptor<BigDecimal> minAmountDescriptor = new ValueStateDescriptor<>(
            "min-amount",
            BigDecimal.class
        );
        minAmount = getRuntimeContext().getState(minAmountDescriptor);
        
        ValueStateDescriptor<BigDecimal> maxAmountDescriptor = new ValueStateDescriptor<>(
            "max-amount",
            BigDecimal.class
        );
        maxAmount = getRuntimeContext().getState(maxAmountDescriptor);
        
        ValueStateDescriptor<Map<String, Integer>> typeCountsDescriptor = new ValueStateDescriptor<>(
            "transaction-type-counts",
            TypeInformation.of(new TypeHint<Map<String, Integer>>() {})
        );
        transactionTypeCounts = getRuntimeContext().getState(typeCountsDescriptor);
        
        ValueStateDescriptor<Map<String, Integer>> locationCountsDescriptor = new ValueStateDescriptor<>(
            "location-counts",
            TypeInformation.of(new TypeHint<Map<String, Integer>>() {})
        );
        locationCounts = getRuntimeContext().getState(locationCountsDescriptor);
        
        ValueStateDescriptor<Map<String, Integer>> deviceCountsDescriptor = new ValueStateDescriptor<>(
            "device-counts",
            TypeInformation.of(new TypeHint<Map<String, Integer>>() {})
        );
        deviceCounts = getRuntimeContext().getState(deviceCountsDescriptor);
    }
    
    @Override
    public void process(String customerId, Context context, Iterable<Transaction> transactions, Collector<TransactionMetrics> out) throws Exception {
        LOG.info("Processing window for customer: {}, window: {} - {}", 
                customerId, context.window().getStart(), context.window().getEnd());
        
        // Initialize window state
        initializeWindowState();
        
        int transactionCountInWindow = 0;
        // Process all transactions in the window
        for (Transaction transaction : transactions) {
            updateTransactionMetrics(transaction);
            transactionCountInWindow++;
            LOG.debug("Processing transaction {} for customer {}: amount={}", 
                    transaction.getId(), customerId, transaction.getAmount());
        }
        
        LOG.info("Found {} transactions in window for customer {}", transactionCountInWindow, customerId);
        
        // Generate final metrics for the window
        TransactionMetrics metrics = generateWindowMetrics(customerId, context.window().getEnd());
        out.collect(metrics);
        
        LOG.info("Emitted TransactionMetrics for customer {}: {} transactions, total: ${}, min: ${}, max: ${}", 
                customerId, transactionCount.value(), totalAmount.value(), 
                metrics.getMinAmount(), metrics.getMaxAmount());
    }
    
    private void initializeWindowState() throws Exception {
        if (totalAmount.value() == null) {
            totalAmount.update(BigDecimal.ZERO);
        }
        if (transactionCount.value() == null) {
            transactionCount.update(0);
        }
        if (minAmount.value() == null) {
            minAmount.update(null); // Initialize as null, not ZERO
        }
        if (maxAmount.value() == null) {
            maxAmount.update(null); // Initialize as null, not ZERO
        }
        if (transactionTypeCounts.value() == null) {
            transactionTypeCounts.update(new HashMap<>());
        }
        if (locationCounts.value() == null) {
            locationCounts.update(new HashMap<>());
        }
        if (deviceCounts.value() == null) {
            deviceCounts.update(new HashMap<>());
        }
    }
    
    private void updateTransactionMetrics(Transaction transaction) throws Exception {
        BigDecimal currentTotal = totalAmount.value();
        Integer currentCount = transactionCount.value();
        BigDecimal currentMin = minAmount.value();
        BigDecimal currentMax = maxAmount.value();
        Map<String, Integer> typeCounts = transactionTypeCounts.value();
        Map<String, Integer> locationCounts = this.locationCounts.value();
        Map<String, Integer> deviceCounts = this.deviceCounts.value();
        
        // Update basic metrics
        BigDecimal amount = transaction.getAmount();
        currentTotal = currentTotal.add(amount);
        currentCount++;
        
        // Update min/max - Fix logic for proper min/max calculation
        if (currentMin == null || amount.compareTo(currentMin) < 0) {
            currentMin = amount;
        }
        if (currentMax == null || amount.compareTo(currentMax) > 0) {
            currentMax = amount;
        }
        
        // Update transaction type counts
        String transactionType = transaction.getTransactionType();
        typeCounts.put(transactionType, typeCounts.getOrDefault(transactionType, 0) + 1);
        
        // Update location counts
        if (transaction.getTransactionLocation() != null) {
            String country = (String) transaction.getTransactionLocation().get("country");
            if (country != null) {
                locationCounts.put(country, locationCounts.getOrDefault(country, 0) + 1);
            }
        }
        
        // Update device counts
        if (transaction.getDeviceFingerprint() != null) {
            deviceCounts.put(transaction.getDeviceFingerprint(), 
                           deviceCounts.getOrDefault(transaction.getDeviceFingerprint(), 0) + 1);
        }
        
        // Update state
        totalAmount.update(currentTotal);
        transactionCount.update(currentCount);
        minAmount.update(currentMin);
        maxAmount.update(currentMax);
        transactionTypeCounts.update(typeCounts);
        this.locationCounts.update(locationCounts);
        this.deviceCounts.update(deviceCounts);
    }
    
    private TransactionMetrics generateWindowMetrics(String customerId, long windowEnd) throws Exception {
        BigDecimal total = totalAmount.value();
        Integer count = transactionCount.value();
        BigDecimal min = minAmount.value();
        BigDecimal max = maxAmount.value();
        Map<String, Integer> typeCounts = transactionTypeCounts.value();
        Map<String, Integer> locationCounts = this.locationCounts.value();
        Map<String, Integer> deviceCounts = this.deviceCounts.value();
        
        // Handle null min/max values
        if (min == null) min = BigDecimal.ZERO;
        if (max == null) max = BigDecimal.ZERO;
        
        // Calculate derived metrics
        BigDecimal averageAmount = count > 0 ? total.divide(BigDecimal.valueOf(count), 2, java.math.RoundingMode.HALF_UP) : BigDecimal.ZERO;
        String preferredTransactionType = findPreferredTransactionType(typeCounts);
        String preferredLocation = findPreferredLocation(locationCounts);
        String preferredDevice = findPreferredDevice(deviceCounts);
        
        // Calculate transaction velocity (transactions per hour)
        double transactionVelocity = count > 0 ? count * 12.0 : 0.0; // 5-minute window * 12 = 1 hour
        
        // Calculate risk indicators
        double riskScore = calculateSimpleRiskScore(total, count, min, max);
        
        // Create metrics
        TransactionMetrics metrics = new TransactionMetrics(
            customerId,
            new Date(windowEnd),
            total,
            count,
            averageAmount,
            min,
            max,
            preferredTransactionType,
            preferredLocation,
            preferredDevice,
            transactionVelocity,
            riskScore
        );
        
        // Set additional metrics
        metrics.setTransactionTypeCounts(typeCounts);
        metrics.setLocationCounts(locationCounts);
        metrics.setDeviceCounts(deviceCounts);
        metrics.setRecentTransactionCount(count);
        metrics.setLastTransactionAmount(max); // Use max as last transaction amount for window
        metrics.setLastTransactionType(preferredTransactionType);
        
        return metrics;
    }
    
    private String findPreferredTransactionType(Map<String, Integer> typeCounts) {
        return typeCounts.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse("unknown");
    }
    
    private String findPreferredLocation(Map<String, Integer> locationCounts) {
        return locationCounts.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse("unknown");
    }
    
    private String findPreferredDevice(Map<String, Integer> deviceCounts) {
        return deviceCounts.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse("unknown");
    }
    

    

    
    private double calculateSimpleRiskScore(BigDecimal total, Integer count, BigDecimal min, BigDecimal max) {
        double riskScore = 0.0;
        
        // High total amount risk
        if (total.compareTo(new BigDecimal("100000")) > 0) {
            riskScore += 25.0;
        }
        
        // High frequency risk
        if (count > 50) {
            riskScore += 15.0;
        }
        
        // High individual transaction risk
        if (max.compareTo(new BigDecimal("10000")) > 0) {
            riskScore += 20.0;
        }
        
        // High average amount risk
        if (count > 0) {
            BigDecimal average = total.divide(BigDecimal.valueOf(count), 2, java.math.RoundingMode.HALF_UP);
            if (average.compareTo(new BigDecimal("5000")) > 0) {
                riskScore += 15.0;
            }
        }
        
        return Math.min(100.0, riskScore);
    }
}