package com.safeway.ossl.storepipeline.service;

import com.safeway.ossl.storepipeline.document.StoreDocument;
import com.safeway.ossl.storepipeline.dto.StoreComparisonResult;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.entity.StoreEntity;
import com.safeway.ossl.storepipeline.exception.StoreNotFoundException;
import com.safeway.ossl.storepipeline.mapper.StoreMapper;
import com.safeway.ossl.storepipeline.repository.StoreMongoRepository;
import com.safeway.ossl.storepipeline.repository.StoreSqlRepository;
import com.safeway.ossl.storepipeline.utility.StoreComparisonUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class StoreQueryService {

    @Autowired
    private StoreSqlRepository sqlRepository;
    @Autowired
    private StoreMongoRepository mongoRepository;
    @Autowired
    private StoreCacheService cacheService;

    private static final Logger log = LoggerFactory.getLogger(StoreQueryService.class);

    public StoreRecord getFromSql(String locationId) {
        StoreEntity dbRecord = sqlRepository.findById(locationId)
                .orElseThrow(() -> StoreNotFoundException.forStore(locationId, "SQL database"));

        log.info("Retrieved storeId={} from SQL database at version={}", locationId, dbRecord.getVersion());
        return StoreMapper.toRecord(dbRecord);
    }

    public StoreRecord getFromMongo(String locationId) {
        StoreDocument dbRecord = mongoRepository.findByLocationId(locationId)
                .orElseThrow(() -> StoreNotFoundException.forStore(locationId, "MongoDB"));

        log.info("Retrieved storeId={} from MongoDB at version={}", locationId, dbRecord.getVersion());

        return StoreMapper.toRecord(dbRecord);
    }

    public StoreRecord getFromCache(String locationId) {
        log.debug("Looking up storeId={} via cache", locationId);
        return cacheService.get(locationId)
                .orElseThrow(() -> StoreNotFoundException.forStore(locationId, "cache or MongoDB"));
    }


    public StoreComparisonResult compare(String locationId) {
        log.info("Comparing SQL and MongoDB copies for storeId={}", locationId);
        Optional<StoreEntity> sql = sqlRepository.findById(locationId);
        Optional<StoreDocument> mongo = mongoRepository.findByLocationId(locationId);
        if (sql.isEmpty() && mongo.isEmpty()) {
            log.warn("Comparison failed: storeId={} missing from both SQL and MongoDB", locationId);
        }
        if (sql.isEmpty() && mongo.isEmpty()) throw StoreNotFoundException.forStore(locationId, "either database");
        Map<String, Object> left = sql.map(e -> StoreComparisonUtility.fields(StoreMapper.toRecord(e))).orElseGet(LinkedHashMap::new);
        Map<String, Object> right = mongo.map(e -> StoreComparisonUtility.fields(StoreMapper.toRecord(e))).orElseGet(LinkedHashMap::new);
        List<String> differences = new ArrayList<>();
        if (sql.isEmpty()) differences.add("__missing_in_sql__");
        if (mongo.isEmpty()) differences.add("__missing_in_mongo__");
        differences.addAll(StoreComparisonUtility.differingKeys(left, right));
        StoreComparisonResult result = new StoreComparisonResult();
        result.setStoreId(locationId);
        result.setSqlValues(left);
        result.setMongoValues(right);
        result.setMismatchedFields(differences);
        result.setConsistent(sql.isPresent() && mongo.isPresent() && differences.isEmpty());
        log.info("Comparison complete for storeId={}: consistent={} mismatchedFields={}",
                locationId, result.isConsistent(), differences);
        return result;
    }
}
