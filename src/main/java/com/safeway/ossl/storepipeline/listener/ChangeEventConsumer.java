package com.safeway.ossl.storepipeline.listener;

import com.safeway.ossl.storepipeline.dto.StoreChangeEvent;
import com.safeway.ossl.storepipeline.service.StoreCacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ChangeEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ChangeEventConsumer.class);

    private final StoreCacheService cacheService;

    public ChangeEventConsumer(StoreCacheService cacheService) {
        this.cacheService = cacheService;
    }

    @KafkaListener(
            topics = "${kafka.topic.change-events}",
            groupId = "${kafka.consumer.group-id}",
            containerFactory = "storeChangeEventKafkaListenerContainerFactory")
    public void onStoreChanged(StoreChangeEvent event) {
        if (event == null || event.getLocationId() == null || event.getLocationId().isBlank()) {
            log.warn("Received null/invalid change event, skipping: {}", event);
            return;
        }

        try {
    cacheService.evict(event.getLocationId());
    cacheService.refreshFromMongo(event.getLocationId());

    log.info("Refreshed cache entry for locationId={} after change event (version={})",
            event.getLocationId(), event.getVersion());
} catch (Exception e) {
    log.error("Failed to process change event for locationId={}: {}",
            event.getLocationId(), e.getMessage(), e);
}
    }
}
