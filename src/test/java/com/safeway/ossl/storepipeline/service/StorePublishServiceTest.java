package com.safeway.ossl.storepipeline.service;

import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.entity.StoreEntity;
import com.safeway.ossl.storepipeline.exception.StoreNotFoundException;
import com.safeway.ossl.storepipeline.repository.StoreSqlRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StorePublishServiceTest {

    private StoreSqlRepository sqlRepository;
    private StoreEventProducerService storeEventProducerService;
    private StorePublishService storePublishService;

    @BeforeEach
    void setUp() {
        sqlRepository = Mockito.mock(StoreSqlRepository.class);
        storeEventProducerService = Mockito.mock(StoreEventProducerService.class);
        storePublishService = new StorePublishService();
        ReflectionTestUtils.setField(storePublishService, "sqlRepository", sqlRepository);
        ReflectionTestUtils.setField(storePublishService, "storeEventProducerService", storeEventProducerService);
    }

    @Test
    void publish_incrementsVersionSavesAndPublishesRecord() {
        StoreEntity existing = new StoreEntity();
        existing.setLocationId("1001");
        existing.setVersion(null);
        existing.setPhone("555-1111");

        when(sqlRepository.findById("1001")).thenReturn(Optional.of(existing));
        when(sqlRepository.save(any(StoreEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        StoreRecord result = storePublishService.publish("1001");

        ArgumentCaptor<StoreEntity> savedCaptor = ArgumentCaptor.forClass(StoreEntity.class);
        verify(sqlRepository).save(savedCaptor.capture());
        assertEquals(1L, savedCaptor.getValue().getVersion());

        ArgumentCaptor<StoreRecord> publishedCaptor = ArgumentCaptor.forClass(StoreRecord.class);
        verify(storeEventProducerService).publish(publishedCaptor.capture());
        assertEquals("1001", publishedCaptor.getValue().getLocationId());
        assertEquals(1L, publishedCaptor.getValue().getVersion());

        assertEquals("1001", result.getLocationId());
        assertEquals(1L, result.getVersion());
    }

    @Test
    void publish_throwsWhenStoreDoesNotExist() {
        when(sqlRepository.findById("missing")).thenReturn(Optional.empty());

        assertThrows(StoreNotFoundException.class, () -> storePublishService.publish("missing"));

        verify(sqlRepository, never()).save(any(StoreEntity.class));
        verify(storeEventProducerService, never()).publish(any(StoreRecord.class));
    }
}

