package com.safeway.ossl.storepipeline.dto;

import com.safeway.ossl.storepipeline.listener.StoreEventConsumer;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Lightweight "store changed" event published to the
 * {@code ossl-store-change-events} Kafka topic (Topic 2) by
 * {@link StoreEventConsumer}
 * after it successfully upserts a store into MongoDB. Consumed by
 * {@link com.safeway.ossl.storepipeline.listener.ChangeEventConsumer} to
 * evict the stale Caffeine cache entry for that store.
 * <p>
 * Deliberately NOT the full store payload — just enough to identify what
 * changed and at what version, keeping the cache-refresh signal small and
 * the two Kafka topics' responsibilities clearly separated per the LLD.
 */
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
