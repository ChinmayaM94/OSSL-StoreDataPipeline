package com.safeway.ossl.storepipeline.service;

import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.entity.StoreEntity;
import com.safeway.ossl.storepipeline.exception.StoreNotFoundException;
import com.safeway.ossl.storepipeline.mapper.StoreMapper;
import com.safeway.ossl.storepipeline.repository.StoreSqlRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;


@Service
public class StorePublishService {

    private static final Logger log = LoggerFactory.getLogger(StorePublishService.class);

    private final StoreSqlRepository sqlRepository;

    private final StoreEventProducerService storeEventProducerService;

    public StorePublishService(StoreSqlRepository sqlRepository, StoreEventProducerService storeEventProducerService) {
        this.sqlRepository = sqlRepository;
        this.storeEventProducerService = storeEventProducerService;
    }

    public StoreRecord publish(String storeId) {
        log.info("Publishing storeId={} from SQL to Kafka", storeId);
        StoreEntity dbRecord = sqlRepository.findById(storeId)
                .orElseThrow(() -> StoreNotFoundException.forStore(storeId, "the SQL database"));

        long nextVersion = (dbRecord.getVersion() == null ? 0L : dbRecord.getVersion()) + 1;
        log.debug("Bumping storeId={} from version={} to version={}", storeId, dbRecord.getVersion(), nextVersion);
        dbRecord.setVersion(nextVersion);
        StoreEntity saved = sqlRepository.save(dbRecord);

        StoreRecord record = StoreMapper.toRecord(saved);
        storeEventProducerService.publish(record);
        log.info("Published storeId={} to ossl-store-event topic at version={}", storeId, nextVersion);

        return record;
    }
}
