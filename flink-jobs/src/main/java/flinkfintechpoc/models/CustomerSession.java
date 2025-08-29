package flinkfintechpoc.models;

import java.util.Date;
import java.util.Map;

/**
 * CustomerSession model for Flink processing
 */
public class CustomerSession {
    private String id;
    private String customerId;
    private String sessionId;
    private String channel;
    private String deviceType;
    private String ipAddress;
    private Map<String, Object> location;
    private Date startedAt;
    private Date endedAt;
    private int actionsCount;
    private int transactionsCount;
    private Date createdAt;
    private Date updatedAt;
    
    // Constructors
    public CustomerSession() {}
    
    public CustomerSession(String id, String customerId, String sessionId, String channel) {
        this.id = id;
        this.customerId = customerId;
        this.sessionId = sessionId;
        this.channel = channel;
        this.createdAt = new Date();
        this.updatedAt = new Date();
    }
    
    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    
    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }
    
    public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }
    
    public String getDeviceType() { return deviceType; }
    public void setDeviceType(String deviceType) { this.deviceType = deviceType; }
    
    public String getIpAddress() { return ipAddress; }
    public void setIpAddress(String ipAddress) { this.ipAddress = ipAddress; }
    
    public Map<String, Object> getLocation() { return location; }
    public void setLocation(Map<String, Object> location) { this.location = location; }
    
    public Date getStartedAt() { return startedAt; }
    public void setStartedAt(Date startedAt) { this.startedAt = startedAt; }
    
    public Date getEndedAt() { return endedAt; }
    public void setEndedAt(Date endedAt) { this.endedAt = endedAt; }
    
    public int getActionsCount() { return actionsCount; }
    public void setActionsCount(int actionsCount) { this.actionsCount = actionsCount; }
    
    public int getTransactionsCount() { return transactionsCount; }
    public void setTransactionsCount(int transactionsCount) { this.transactionsCount = transactionsCount; }
    
    public Date getCreatedAt() { return createdAt; }
    public void setCreatedAt(Date createdAt) { this.createdAt = createdAt; }
    
    public Date getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Date updatedAt) { this.updatedAt = updatedAt; }
    
    @Override
    public String toString() {
        return "CustomerSession{" +
                "id='" + id + '\'' +
                ", customerId='" + customerId + '\'' +
                ", sessionId='" + sessionId + '\'' +
                ", channel='" + channel + '\'' +
                ", deviceType='" + deviceType + '\'' +
                ", startedAt=" + startedAt +
                ", endedAt=" + endedAt +
                ", actionsCount=" + actionsCount +
                ", transactionsCount=" + transactionsCount +
                '}';
    }
}
