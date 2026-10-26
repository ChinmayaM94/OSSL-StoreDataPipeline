package com.safeway.ossl.storepipeline.utility;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.safeway.ossl.storepipeline.dto.RowError;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.entity.StoreEntity;
import com.safeway.ossl.storepipeline.exception.KafkaPublishException;
import com.safeway.ossl.storepipeline.repository.StoreSqlRepository;
import com.safeway.ossl.storepipeline.service.StoreEventProducerService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Publishes a batch of validated store rows to Kafka. Reusable by any service that needs to
 * push parsed store records through the producer with correct versioning.
 */
@Component
@RequiredArgsConstructor
public class StoreBatchPublishUtility {

    private static final Logger log = LoggerFactory.getLogger(StoreBatchPublishUtility.class);

    private final StoreEventProducerService producer;
    private final StoreSqlRepository sqlRepository;
    private final MeterRegistry meterRegistry;

    /** A validated row waiting to be published. */
    public record PendingRow(int rowNumber, String id, ObjectNode payload) {}

    /**
     * Looks up versions in one query, fires every send, then waits for all acks together.
     * Failed rows are appended to {@code errors}.
     *
     * @return the number of rows published successfully
     */
    public int publishAll(List<PendingRow> pending, List<RowError> errors) {
        if (pending.isEmpty()) {
            log.info("No rows to publish");
            return 0;
        }
        log.info("Publishing batch of {} store rows", pending.size());

        Map<String, Long> currentVersions = new HashMap<>();
        for (StoreEntity entity : sqlRepository.findAllById(pending.stream().map(PendingRow::id).toList())) {
            currentVersions.put(entity.getLocationId(), entity.getVersion());
        }

        log.debug("Resolved current versions for {} of {} stores", currentVersions.size(), pending.size());
        List<CompletableFuture<Void>> futures = new ArrayList<>(pending.size());
        for (PendingRow row : pending) {
            long version = 0L; // new store
            if (currentVersions.containsKey(row.id())) {
                Long current = currentVersions.get(row.id());
                version = current == null ? 1L : current + 1;
            }
            futures.add(producer.publishAsync(new StoreRecord(row.id(), row.payload(), version)));
        }

        int processed = 0;
        for (int i = 0; i < pending.size(); i++) {
            try {
                futures.get(i).join();
                processed++;
                meterRegistry.counter("ossl.store.publish", "result", "success").increment();
            } catch (Exception ex) {
                Throwable cause = ex instanceof CompletionException && ex.getCause() != null ? ex.getCause() : ex;
                String field = cause instanceof KafkaPublishException ? "kafka" : "csv";
                log.warn("Publish failed for row={} storeId={} field={}: {}", pending.get(i).rowNumber(),
                        pending.get(i).id(), field, ExceptionUtility.errorMessage(cause));
                errors.add(new RowError(pending.get(i).rowNumber(), field, ExceptionUtility.errorMessage(cause)));
                meterRegistry.counter("ossl.store.publish", "result", "failure", "reason", field).increment();
            }
        }

        log.info("Batch publish finished: published={} failed={}", processed, pending.size() - processed);
        return processed;
    }
}
