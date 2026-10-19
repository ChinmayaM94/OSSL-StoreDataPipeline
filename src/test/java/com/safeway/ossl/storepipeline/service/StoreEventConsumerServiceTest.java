package com.safeway.ossl.storepipeline.service;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.entity.StoreEntity;
import com.safeway.ossl.storepipeline.repository.StoreSqlRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StoreEventConsumerServiceTest {

    private StoreSqlRepository storeSqlRepository;
    private StoreEventConsumerService consumerService;

    @BeforeEach
    void setUp() {
        storeSqlRepository = Mockito.mock(StoreSqlRepository.class);
        consumerService = new StoreEventConsumerService(storeSqlRepository);
    }

    @Test
    void consume_mapsStoreRecordToEntityAndSaves() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("locationId", "1001");
        payload.putArray("departments").add("grocery").add("pharmacy");
        payload.put("isEcomStore", true);
        payload.put("timezone", "PST");
        payload.putObject("brand").put("name", "Safeway");
        payload.putObject("division").put("name", "Northwest");
        payload.put("phone", "555-1111");
        payload.put("latitude", 47.61);
        payload.put("longitude", -122.33);
        payload.putObject("address")
                .put("line1", "123 Main")
                .put("city", "Seattle")
                .put("state", "WA")
                .put("zipcode", "98101");
        payload.putObject("ecomStore")
                .put("operatingStatus", "ACTIVE")
                .put("isDUGStore", true)
                .put("isDeliveryStore", false)
                .putObject("storeFeatures")
                .put("isExpressStore", true)
                .put("isWGDEnabled", false)
                .put("isFlashDugEnabled", true)
                .put("isFlashDeliveryEnabled", false);

        when(storeSqlRepository.findById("1001")).thenReturn(Optional.empty());
        when(storeSqlRepository.save(any(StoreEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        consumerService.consume(new StoreRecord("1001", payload, 0L));

        ArgumentCaptor<StoreEntity> captor = ArgumentCaptor.forClass(StoreEntity.class);
        verify(storeSqlRepository, times(1)).save(captor.capture());

        StoreEntity saved = captor.getValue();
        assertEquals("1001", saved.getLocationId());
        assertEquals("grocery, pharmacy", saved.getDepartments());
        assertEquals(Boolean.TRUE, saved.getIsEcomStore());
        assertEquals("PST", saved.getTimezone());
        assertEquals("Safeway", saved.getBrandName());
        assertEquals("Northwest", saved.getDivisionName());
        assertEquals("555-1111", saved.getPhone());
        assertEquals(47.61, saved.getLatitude());
        assertEquals(-122.33, saved.getLongitude());
    }

    @Test
    void consume_isIdempotentForReplayedRecord() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("locationId", "1001");

        StoreEntity existing = new StoreEntity();
        existing.setLocationId("1001");
        existing.setVersion(3L);

        when(storeSqlRepository.findById("1001")).thenReturn(Optional.of(existing));
        when(storeSqlRepository.save(any(StoreEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        StoreRecord record = new StoreRecord("1001", payload, 0L);

        consumerService.consume(record);
        consumerService.consume(record);

        verify(storeSqlRepository, times(2)).save(any(StoreEntity.class));
        assertEquals(3L, existing.getVersion(), "a lower incoming version must not move the SQL version backwards");
    }
}