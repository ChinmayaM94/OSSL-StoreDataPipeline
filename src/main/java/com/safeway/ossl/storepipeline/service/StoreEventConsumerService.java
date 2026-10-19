package com.safeway.ossl.storepipeline.service;

import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.entity.StoreEntity;
import com.safeway.ossl.storepipeline.mapper.StoreMapper;
import com.safeway.ossl.storepipeline.repository.StoreSqlRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.atomic.AtomicLong;

@Service
public class StoreEventConsumerService {

    private static final Logger log = LoggerFactory.getLogger(StoreEventConsumerService.class);

    private static final long LOG_EVERY_N_RECORDS = 100;

    private final StoreSqlRepository storeSqlRepository;
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);

    public StoreEventConsumerService(StoreSqlRepository storeSqlRepository) {
        this.storeSqlRepository = storeSqlRepository;
    }

    @KafkaListener(
            topics = "${kafka.topic.store-events}",
            groupId = "${kafka.consumer.sql-group-id}",
            containerFactory = "storeRecordKafkaListenerContainerFactory"
    )
    @Transactional
    public void consume(StoreRecord record) {
        String storeId = record != null ? record.getLocationId() : null;

        if (storeId == null || storeId.isBlank()) {
            long failures = failedCount.incrementAndGet();
            log.error("Failed to persist Kafka event due to missing storeId. failedCount={}", failures);
            throw new IllegalArgumentException("store event must contain a locationId");
        }

        try {
            StoreEntity existing = storeSqlRepository.findById(storeId).orElse(null);
            StoreEntity entity = StoreMapper.toEntity(record, existing);
            storeSqlRepository.save(entity);

            long processed = processedCount.incrementAndGet();

            if (processed % LOG_EVERY_N_RECORDS == 0) {
                log.info("Kafka consumer progress: processedCount={}, failedCount={}", processed, failedCount.get());
            }
        } catch (Exception ex) {
            long failures = failedCount.incrementAndGet();
            log.error("Failed to persist storeId={} from Kafka event. failedCount={}", storeId, failures, ex);
            throw ex;
        }
    }

}