package com.safeway.ossl.storepipeline.service;

import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.entity.StoreEntity;
import com.safeway.ossl.storepipeline.exception.StoreNotFoundException;
import com.safeway.ossl.storepipeline.mapper.StoreMapper;
import com.safeway.ossl.storepipeline.repository.StoreSqlRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Orchestrates {@code POST /api/stores/{storeId}/publish}: reads the store
 * from the SQL datastore, assigns the next version, persists that version
 * back to SQL, and publishes the versioned record to Kafka Topic 1 via
 * {@link StoreEventProducerService}.
 */
@Service
public class StorePublishService {

    private static final Logger log = LoggerFactory.getLogger(StorePublishService.class);

    @Autowired
    private StoreSqlRepository sqlRepository;

    @Autowired
    private StoreEventProducerService storeEventProducerService;

    public StoreRecord publish(String storeId) {
        StoreEntity dbRecord = sqlRepository.findById(storeId)
                .orElseThrow(() -> new StoreNotFoundException(
                        "Store '" + storeId + "' was not found in the SQL database."));

        long nextVersion = (dbRecord.getVersion() == null ? 0L : dbRecord.getVersion()) + 1;
        dbRecord.setVersion(nextVersion);
        StoreEntity saved = sqlRepository.save(dbRecord);

        StoreRecord record = StoreMapper.toRecord(saved);
        storeEventProducerService.publish(record);
        log.info("Published storeId={} to ossl-store-event topic at version={}", storeId, nextVersion);

        return record;
    }
}
