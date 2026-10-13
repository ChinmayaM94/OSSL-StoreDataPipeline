package com.safeway.ossl.storepipeline.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.safeway.ossl.storepipeline.document.StoreDocument;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.repository.StoreMongoRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class StoreCacheServiceTest {

    private Cache<String, StoreRecord> cache;
    private StoreMongoRepository mongoRepository;
    private StoreCacheService storeCacheService;

    @BeforeEach
    void setUp() {
        cache = Caffeine.newBuilder().build();
        mongoRepository = Mockito.mock(StoreMongoRepository.class);
        storeCacheService = new StoreCacheService(cache, mongoRepository);
    }

    @Test
    void get_returnsCachedRecordWithoutMongoLookup() {
        StoreRecord cached = new StoreRecord("1001", null, 2L);
        storeCacheService.put("1001", cached);

        Optional<StoreRecord> result = storeCacheService.get("1001");

        assertTrue(result.isPresent());
        assertEquals("1001", result.get().getLocationId());
        assertEquals(2L, result.get().getVersion());
        verifyNoInteractions(mongoRepository);
    }

    @Test
    void get_loadsFromMongoOnMissAndCachesForNextRead() {
        when(mongoRepository.findByLocationId("1001")).thenReturn(Optional.of(document("1001", "Seattle", 4L)));

        Optional<StoreRecord> first = storeCacheService.get("1001");
        Optional<StoreRecord> second = storeCacheService.get("1001");

        assertTrue(first.isPresent());
        assertTrue(second.isPresent());
        assertEquals("Seattle", first.get().getStoreInfo().path("address").path("city").asText());
        assertEquals(4L, first.get().getVersion());
        verify(mongoRepository, times(1)).findByLocationId("1001");
    }

    @Test
    void evict_removesCachedEntry() {
        storeCacheService.put("1001", new StoreRecord("1001", null, 1L));

        storeCacheService.evict("1001");

        assertNotNull(cache.asMap());
        assertFalse(cache.asMap().containsKey("1001"));
    }

    @Test
    void refreshFromMongo_replacesCachedEntryWhenDocumentExists() {
        storeCacheService.put("1001", new StoreRecord("1001", null, 1L));
        when(mongoRepository.findByLocationId("1001")).thenReturn(Optional.of(document("1001", "Phoenix", 5L)));

        storeCacheService.refreshFromMongo("1001");

        StoreRecord refreshed = cache.getIfPresent("1001");
        assertNotNull(refreshed);
        assertEquals(5L, refreshed.getVersion());
        assertEquals("Phoenix", refreshed.getStoreInfo().path("address").path("city").asText());
    }

    private static StoreDocument document(String locationId, String city, long version) {
        StoreDocument document = new StoreDocument();
        document.setId("doc-" + locationId);
        document.setLocationId(locationId);
        document.setVersion(version);
        document.setLastUpdated(Instant.now());
        document.setStoreInfo(Map.of(
                "address", Map.of("city", city),
                "phone", "555-1111"
        ));
        return document;
    }
}

