package com.safeway.ossl.storepipeline.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Signals that a store was upserted into MongoDB and its cache entry should be refreshed")
public class StoreChangeEvent {

    @Schema(description = "Store identifier that changed.", example = "1001")
    private String locationId;

    @Schema(description = "Version of the store record that was applied.", example = "3")
    private Long version;

    @Schema(description = "Timestamp the change was applied in MongoDB.")
    private Instant changeTimestamp;
}
