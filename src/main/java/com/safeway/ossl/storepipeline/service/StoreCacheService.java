package com.safeway.ossl.storepipeline.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.mapper.StoreMapper;
import com.safeway.ossl.storepipeline.repository.StoreMongoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class StoreCacheService {

    private static final Logger log = LoggerFactory.getLogger(StoreCacheService.class);

    private final Cache<String, StoreRecord> cache;
    private final StoreMongoRepository mongoRepository;

    public StoreCacheService(Cache<String, StoreRecord> storeRecordCache, StoreMongoRepository mongoRepository) {
        this.cache = storeRecordCache;
        this.mongoRepository = mongoRepository;
    }

    /**
     * Cache-aside read: returns the cached value if present, otherwise loads
     * from MongoDB and populates the cache before returning.
     */
    public Optional<StoreRecord> get(String storeId) {
        StoreRecord cached = cache.getIfPresent(storeId);
        if (cached != null) {
            log.info("Cache hit for storeId={}", storeId);
            return Optional.of(cached);
        }

        log.info("Cache miss for storeId={}, loading from MongoDB", storeId);
        return mongoRepository.findByLocationId(storeId).map(doc -> {
            StoreRecord record = StoreMapper.toRecord(doc);
            cache.put(storeId, record);
            return record;
        });
    }

    public void put(String storeId, StoreRecord record) {
        cache.put(storeId, record);
    }

    public void evict(String storeId) {
        cache.invalidate(storeId);
        log.debug("Evicted cache entry for storeId={}", storeId);
    }

    public void refreshFromMongo(String storeId) {
        mongoRepository.findByLocationId(storeId).ifPresent(doc -> {
            StoreRecord record = StoreMapper.toRecord(doc);
            cache.put(storeId, record);
            log.debug("Refreshed cache entry for storeId={} from MongoDB", storeId);
        });
    }
}
