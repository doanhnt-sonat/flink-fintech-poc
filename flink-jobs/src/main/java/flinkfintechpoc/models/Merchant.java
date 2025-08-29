package flinkfintechpoc.models;

import java.util.Date;

/**
 * Merchant model for Flink processing
 */
public class Merchant {
    private String id;
    private String name;
    private String businessType;
    private String mccCode;
    private String address;
    private String phone;
    private String website;
    private String taxId;
    private boolean isActive;
    private Date createdAt;
    private Date updatedAt;
    
    // Constructors
    public Merchant() {}
    
    public Merchant(String id, String name, String businessType, String mccCode) {
        this.id = id;
        this.name = name;
        this.businessType = businessType;
        this.mccCode = mccCode;
        this.isActive = true;
        this.createdAt = new Date();
        this.updatedAt = new Date();
    }
    
    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public String getBusinessType() { return businessType; }
    public void setBusinessType(String businessType) { this.businessType = businessType; }
    
    public String getMccCode() { return mccCode; }
    public void setMccCode(String mccCode) { this.mccCode = mccCode; }
    
    public String getAddress() { return address; }
    public void setAddress(String address) { this.address = address; }
    
    public String getPhone() { return phone; }
    public void setPhone(String phone) { this.phone = phone; }
    
    public String getWebsite() { return website; }
    public void setWebsite(String website) { this.website = website; }
    
    public String getTaxId() { return taxId; }
    public void setTaxId(String taxId) { this.taxId = taxId; }
    
    public boolean isActive() { return isActive; }
    public void setActive(boolean active) { isActive = active; }
    
    public Date getCreatedAt() { return createdAt; }
    public void setCreatedAt(Date createdAt) { this.createdAt = createdAt; }
    
    public Date getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Date updatedAt) { this.updatedAt = updatedAt; }
    
    @Override
    public String toString() {
        return "Merchant{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", businessType='" + businessType + '\'' +
                ", mccCode='" + mccCode + '\'' +
                ", isActive=" + isActive +
                ", createdAt=" + createdAt +
                '}';
    }
}
