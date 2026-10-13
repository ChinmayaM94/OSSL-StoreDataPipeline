package com.safeway.ossl.storepipeline.service;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.repository.StoreSqlRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class StoreEventConsumerServiceFailureTest {

    private StoreSqlRepository storeSqlRepository;
    private StoreEventConsumerService consumerService;

    @BeforeEach
    void setUp() {
        storeSqlRepository = Mockito.mock(StoreSqlRepository.class);
        consumerService = new StoreEventConsumerService(storeSqlRepository);
    }

    @Test
    void consume_propagatesDatabaseFailureForRetryHandling() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("locationId", "1001");

        when(storeSqlRepository.findById("1001")).thenReturn(Optional.empty());
        when(storeSqlRepository.save(any())).thenThrow(new DataIntegrityViolationException("simulated db failure"));

        assertThrows(DataIntegrityViolationException.class,
                () -> consumerService.consume(new StoreRecord("1001", payload, 0L)));
    }
}