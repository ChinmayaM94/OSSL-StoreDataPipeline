package com.safeway.ossl.storepipeline.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.Instant;

/** Mongo _id is always locationId; source _id is retained inside sourceJson, never used as the document key. */
@Document(collection = "stores")
public class StoreDocument {
    @Id
    private String locationId;
    private String sourceJson;
    private Long version;
    private Instant lastUpdated;
    public String getLocationId() { return locationId; }
    public void setLocationId(String locationId) { this.locationId = locationId; }
    public String getSourceJson() { return sourceJson; }
    public void setSourceJson(String sourceJson) { this.sourceJson = sourceJson; }
    public Long getVersion() { return version; }
    public void setVersion(Long version) { this.version = version; }
    public Instant getLastUpdated() { return lastUpdated; }
    public void setLastUpdated(Instant lastUpdated) { this.lastUpdated = lastUpdated; }
}
