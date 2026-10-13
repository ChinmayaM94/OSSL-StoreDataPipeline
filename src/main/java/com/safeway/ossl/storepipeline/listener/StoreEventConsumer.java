package com.safeway.ossl.storepipeline.listener;

import com.safeway.ossl.storepipeline.document.StoreDocument;
import com.safeway.ossl.storepipeline.dto.StoreChangeEvent;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.mapper.StoreMapper;
import com.safeway.ossl.storepipeline.repository.StoreMongoRepository;
import com.safeway.ossl.storepipeline.service.ChangeEventProducerService;
import com.safeway.ossl.storepipeline.service.StoreCacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;

/**
 * Consumes Kafka Topic 1 ({@code ossl-store-events}) and upserts the
 * received {@link StoreRecord} into MongoDB.
 */
@Component
public class StoreEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(StoreEventConsumer.class);

    @Autowired
    private StoreMongoRepository mongoRepository;

    @Autowired
    private ChangeEventProducerService changeEventProducerService;

    @Autowired
    private StoreCacheService cacheService;

    @KafkaListener(
            topics = "${kafka.topic.store-events}",
            groupId = "${kafka.consumer.mongo-group-id}",
            containerFactory = "storeRecordKafkaListenerContainerFactory")
    public void onStorePublished(StoreRecord record) {
        if (record == null || record.getLocationId() == null || record.getLocationId().isBlank()) {
            log.warn("Received null/invalid store event message, skipping: {}", record);
            return;
        }

        try {
            long incomingVersion = record.getVersion() == null ? 0L : record.getVersion();
        Optional<StoreDocument> existing = mongoRepository.findByLocationId(record.getLocationId());

            boolean shouldApply = existing.isEmpty()
                    || existing.get().getVersion() == null
                    || incomingVersion > existing.get().getVersion();

            if (!shouldApply) {
                log.info("Skipping out-of-order/duplicate store event for storeId={} incomingVersion={} storedVersion={}",
                        record.getLocationId(), incomingVersion, existing.get().getVersion());
                return;
            }

            StoreDocument document = StoreMapper.toDocument(record);
            existing.ifPresent(found -> document.setId(found.getId())); // Preserve MongoDB _id if updating
            mongoRepository.save(document);
            cacheService.put(record.getLocationId(), record);

            log.info("Upserted storeId={} into MongoDB at version={}", record.getLocationId(), incomingVersion);

            changeEventProducerService.publish(
                    new StoreChangeEvent(record.getLocationId(), incomingVersion, Instant.now()));

        } catch (Exception e) {
            log.error("Failed to process store event message for storeId={}: {}",
                    record.getLocationId(), e.getMessage(), e);
        }
    }
}
