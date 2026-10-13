package com.safeway.ossl.storepipeline.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.safeway.ossl.storepipeline.document.StoreDocument;
import com.safeway.ossl.storepipeline.dto.StoreComparisonResult;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.entity.StoreEntity;
import com.safeway.ossl.storepipeline.exception.StoreNotFoundException;
import com.safeway.ossl.storepipeline.mapper.StoreMapper;
import com.safeway.ossl.storepipeline.repository.StoreMongoRepository;
import com.safeway.ossl.storepipeline.repository.StoreSqlRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;

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
                .orElseThrow(() -> new StoreNotFoundException("Store '" + locationId + "' not found in SQL database."));

        log.info("Retrieved storeId={} from SQL database at version={}", locationId, dbRecord.getVersion());
        return StoreMapper.toRecord(dbRecord);
    }

    public StoreRecord getFromMongo(String locationId) {
        StoreDocument dbRecord = mongoRepository.findByLocationId(locationId)
                .orElseThrow(() -> new StoreNotFoundException("Store '" + locationId + "' not found in MongoDB."));

        log.info("Retrieved storeId={} from MongoDB at version={}", locationId, dbRecord.getVersion());

        return StoreMapper.toRecord(dbRecord);
    }

    public StoreRecord getFromCache(String locationId) {
        return cacheService.get(locationId)
                .orElseThrow(() -> new StoreNotFoundException("Store '" + locationId + "' not found in cache or MongoDB."));
    }


    public StoreComparisonResult compare(String locationId) {
        Optional<StoreEntity> sql = sqlRepository.findById(locationId);
        Optional<StoreDocument> mongo = mongoRepository.findByLocationId(locationId);
        if (sql.isEmpty() && mongo.isEmpty()) throw new StoreNotFoundException("Store '" + locationId + "' was not found in either database.");
        Map<String, Object> left = sql.map(e -> fields(StoreMapper.toRecord(e))).orElseGet(LinkedHashMap::new);
        Map<String, Object> right = mongo.map(e -> fields(StoreMapper.toRecord(e))).orElseGet(LinkedHashMap::new);
        List<String> differences = new ArrayList<>();
        if (sql.isEmpty()) differences.add("__missing_in_sql__");
        if (mongo.isEmpty()) differences.add("__missing_in_mongo__");
        for (String key : new TreeSet<>(union(left, right))) {
            if (!Objects.equals(left.get(key), right.get(key))) differences.add(key);
        }
        StoreComparisonResult result = new StoreComparisonResult();
        result.setStoreId(locationId);
        result.setSqlValues(left);
        result.setMongoValues(right);
        result.setMismatchedFields(differences);
        result.setConsistent(sql.isPresent() && mongo.isPresent() && differences.isEmpty());
        return result;
    }

    private static java.util.Set<String> union(Map<String, Object> a, Map<String, Object> b) {
        java.util.Set<String> keys = new TreeSet<>(a.keySet()); keys.addAll(b.keySet()); return keys;
    }

    private static Map<String, Object> fields(StoreRecord record) {
        Map<String, Object> map = new LinkedHashMap<>();
        flatten("", record.getStoreInfo(), map);
        map.put("version", record.getVersion());
        return map;
    }

    private static void flatten(String prefix, JsonNode node, Map<String, Object> result) {
        if (node.isObject() && node.size() > 0) node.fields().forEachRemaining(e -> flatten(prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey(), e.getValue(), result));
        else result.put(prefix, node);
    }
}
