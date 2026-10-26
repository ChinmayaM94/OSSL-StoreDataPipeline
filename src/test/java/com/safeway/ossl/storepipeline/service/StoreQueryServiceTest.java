package com.safeway.ossl.storepipeline.service;

import com.safeway.ossl.storepipeline.document.StoreDocument;
import com.safeway.ossl.storepipeline.dto.StoreComparisonResult;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.entity.AddressEntity;
import com.safeway.ossl.storepipeline.entity.StoreEntity;
import com.safeway.ossl.storepipeline.exception.StoreNotFoundException;
import com.safeway.ossl.storepipeline.mapper.StoreMapper;
import com.safeway.ossl.storepipeline.repository.StoreMongoRepository;
import com.safeway.ossl.storepipeline.repository.StoreSqlRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

class StoreQueryServiceTest {

    private StoreSqlRepository sqlRepository;
    private StoreMongoRepository mongoRepository;
    private StoreCacheService cacheService;
    private StoreQueryService storeQueryService;

    @BeforeEach
    void setUp() {
        sqlRepository = Mockito.mock(StoreSqlRepository.class);
        mongoRepository = Mockito.mock(StoreMongoRepository.class);
        cacheService = Mockito.mock(StoreCacheService.class);
        storeQueryService = new StoreQueryService();
        ReflectionTestUtils.setField(storeQueryService, "sqlRepository", sqlRepository);
        ReflectionTestUtils.setField(storeQueryService, "mongoRepository", mongoRepository);
        ReflectionTestUtils.setField(storeQueryService, "cacheService", cacheService);
    }

    @Test
    void getFromSql_returnsMappedStoreRecord() {
        when(sqlRepository.findById("1001")).thenReturn(Optional.of(sqlEntity("1001", "Seattle", 7L)));

        StoreRecord result = storeQueryService.getFromSql("1001");

        assertEquals("1001", result.getLocationId());
        assertEquals(7L, result.getVersion());
        assertEquals("Seattle", result.getStoreInfo().path("address").path("city").asText());
    }

    @Test
    void getFromMongo_returnsMappedStoreRecord() {
        StoreEntity entity = sqlEntity("1001", "Seattle", 7L);
        when(mongoRepository.findByLocationId("1001")).thenReturn(Optional.of(mongoDocument(entity)));

        StoreRecord result = storeQueryService.getFromMongo("1001");

        assertEquals("1001", result.getLocationId());
        assertEquals(7L, result.getVersion());
        assertEquals("Seattle", result.getStoreInfo().path("address").path("city").asText());
    }

    @Test
    void getFromCache_throwsWhenAbsentFromCacheAndMongo() {
        when(cacheService.get("1001")).thenReturn(Optional.empty());

        assertThrows(StoreNotFoundException.class, () -> storeQueryService.getFromCache("1001"));
    }

    @Test
    void compare_returnsConsistentTrueWhenSqlAndMongoMatch() {
        StoreEntity entity = sqlEntity("1001", "Seattle", 3L);
        when(sqlRepository.findById("1001")).thenReturn(Optional.of(entity));
        when(mongoRepository.findByLocationId("1001")).thenReturn(Optional.of(mongoDocument(entity)));

        StoreComparisonResult result = storeQueryService.compare("1001");

        assertTrue(result.isConsistent());
        assertTrue(result.getMismatchedFields().isEmpty());
        assertEquals("Seattle", result.getSqlValues().get("address.city").toString().replace("\"", ""));
        assertEquals("Seattle", result.getMongoValues().get("address.city").toString().replace("\"", ""));
    }

    @Test
    void compare_marksMissingMongoWhenOnlySqlExists() {
        when(sqlRepository.findById("1001")).thenReturn(Optional.of(sqlEntity("1001", "Seattle", 3L)));
        when(mongoRepository.findByLocationId("1001")).thenReturn(Optional.empty());

        StoreComparisonResult result = storeQueryService.compare("1001");

        assertFalse(result.isConsistent());
        assertTrue(result.getMismatchedFields().contains("__missing_in_mongo__"));
        assertFalse(result.getSqlValues().isEmpty());
        assertTrue(result.getMongoValues().isEmpty());
    }

    @Test
    void compare_throwsWhenStoreMissingFromBothSources() {
        when(sqlRepository.findById("1001")).thenReturn(Optional.empty());
        when(mongoRepository.findByLocationId("1001")).thenReturn(Optional.empty());

        assertThrows(StoreNotFoundException.class, () -> storeQueryService.compare("1001"));
    }

    private static StoreEntity sqlEntity(String locationId, String city, long version) {
        StoreEntity entity = new StoreEntity();
        entity.setLocationId(locationId);
        entity.setDepartments("grocery");
        entity.setTimezone("PST");
        entity.setBrandName("Safeway");
        entity.setDivisionName("Northwest");
        entity.setPhone("555-1111");
        entity.setLatitude(47.61);
        entity.setLongitude(-122.33);
        entity.setVersion(version);

        AddressEntity address = new AddressEntity();
        address.setLocationId(locationId);
        address.setStore(entity);
        address.setAddressLine1("123 Main");
        address.setAddressCity(city);
        address.setAddressState("WA");
        address.setAddressZipcode("98101");
        entity.setAddress(address);
        return entity;
    }

    private static StoreDocument mongoDocument(StoreEntity entity) {
        StoreRecord record = StoreMapper.toRecord(entity);
        StoreDocument document = StoreMapper.toDocument(record);
        document.setId("doc-" + entity.getLocationId());
        document.setLastUpdated(Instant.now());
        return document;
    }
}

