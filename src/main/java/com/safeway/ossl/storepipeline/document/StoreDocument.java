package com.safeway.ossl.storepipeline.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.Map;

@Document(collection = "stores")
@Getter
@Setter
public class StoreDocument {

    @Id
    private String id;

    @Indexed(unique = true)
    private String locationId;

    private Map<String, Object> storeInfo;

    private Long version;

    private Instant lastUpdated;
}