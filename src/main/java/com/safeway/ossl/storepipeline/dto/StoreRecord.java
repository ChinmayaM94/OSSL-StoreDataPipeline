package com.safeway.ossl.storepipeline.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

/** The source store profile is retained in full; version is pipeline metadata, not a CSV field. */
@Data
@Schema(description = "Versioned store profile. payload contains source JSON keyed by locationId, including nested ecomStore, address, hours, departmentsDetail and any extensions.")
public class StoreRecord {
    @Schema(example = "1502", requiredMode = Schema.RequiredMode.REQUIRED)
    private String locationId;
    @Schema(description = "Complete source document, including typed nested objects and arrays; source _id and _class are ordinary source values.")
    private ObjectNode payload = JsonNodeFactory.instance.objectNode();
    private Long version;

    public StoreRecord() { }
    public StoreRecord(String locationId, ObjectNode payload, Long version) {
        setLocationId(locationId);
        setPayload(payload);
    }

    @JsonIgnore
    public JsonNode getSourceField(String path) { return payload.path(path); }
}
