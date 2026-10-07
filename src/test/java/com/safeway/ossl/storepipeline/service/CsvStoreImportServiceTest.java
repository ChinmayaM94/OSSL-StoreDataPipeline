package com.safeway.ossl.storepipeline.service;

import com.safeway.ossl.storepipeline.dto.ImportResult;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.entity.StoreEntity;
import com.safeway.ossl.storepipeline.exception.KafkaPublishException;
import com.safeway.ossl.storepipeline.repository.StoreSqlRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.mock.web.MockMultipartFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for CsvStoreImportService. The Kafka producer is mocked —
 * these tests don't require an embedded Kafka broker, per the ticket's
 * testing scope.
 */
class CsvStoreImportServiceTest {

    private static final String CSV_HEADER =
            "locationId,timestamp,latitude,longitude,storeStatus";

    // 4 valid rows + 4 deliberately malformed rows (missing locationId, non-numeric
    // timestamp, non-numeric latitude, out-of-range longitude) = 8 total data rows.
    private static final String CSV_CONTENT = String.join("\n",
            CSV_HEADER,
            "1001,1696600000,47.6097,-122.3331,ACTIVE",
            "1002,1696600001,47.6101,-122.2015,ACTIVE",
            "1003,1696600002,45.5231,-122.6765,ACTIVE",
            ",1696600003,47.2529,-122.4443,ACTIVE",
            "1005,NOTANUMBER,47.6588,-117.4260,ACTIVE",
            "1006,1696600004,NOTANUMBER,-122.9007,ACTIVE",
            "1007,1696600005,47.0379,200,ACTIVE",
            "1008,1696600006,46.6021,-120.5059,CLOSED"
    ) + "\n";

    private StoreEventProducerService producerService;
    private StoreSqlRepository storeSqlRepository;
    private CsvStoreImportService csvStoreImportService;

    @BeforeEach
    void setUp() {
        producerService = Mockito.mock(StoreEventProducerService.class);
        storeSqlRepository = Mockito.mock(StoreSqlRepository.class);
        // SQL upsert is not the focus of these tests — stub it to behave as a normal,
        // always-succeeding upsert (no existing row, save() just echoes back its input)
        // so the pre-existing Kafka-focused assertions in this test class are unaffected.
        lenient().when(storeSqlRepository.findById(anyString())).thenReturn(Optional.empty());
        lenient().when(storeSqlRepository.save(any(StoreEntity.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        csvStoreImportService = new CsvStoreImportService(producerService, storeSqlRepository);
    }

    @Test
    void mixedValidAndMalformedRows_producesCorrectCountsAndDoesNotThrow() throws IOException {
        MockMultipartFile file = new MockMultipartFile(
                "file", "sample_stores.csv", "text/csv",
                CSV_CONTENT.getBytes(StandardCharsets.UTF_8));

        ImportResult result = csvStoreImportService.importCsv(file);

        assertEquals(8, result.getTotalRows(), "total rows should include all 8 data rows");
        assertEquals(4, result.getProcessedCount(), "4 rows are valid and should be processed");
        assertEquals(4, result.getFailureCount(), "4 rows are malformed and should be counted as failures");
        assertEquals(4, result.getErrors().size(), "errors list should have one entry per malformed row");

        // All 4 valid rows must still have been published despite the malformed rows
        // interleaved among them (AC #4 — no silent drops of the healthy rows).
        verify(producerService, times(4)).publish(any(StoreRecord.class));

        ArgumentCaptor<StoreRecord> captor = ArgumentCaptor.forClass(StoreRecord.class);
        verify(producerService, times(4)).publish(captor.capture());
        List<String> publishedIds = captor.getAllValues().stream().map(StoreRecord::getLocationId).toList();
        assertTrue(publishedIds.containsAll(List.of("1001", "1002", "1003", "1008")));

        // Row numbers in errors must account for the header (first data row = 2).
        assertTrue(result.getErrors().stream().anyMatch(e -> e.getRowNumber() == 5)); // missing locationId
        assertTrue(result.getErrors().stream().anyMatch(e -> e.getRowNumber() == 6)); // bad timestamp
        assertTrue(result.getErrors().stream().anyMatch(e -> e.getRowNumber() == 7)); // bad latitude
        assertTrue(result.getErrors().stream().anyMatch(e -> e.getRowNumber() == 8)); // bad longitude (out of range)
    }

    @Test
    void kafkaPublishFailureOnOneRow_isCountedAsFailureAndOtherRowsStillPublish() throws IOException {
        // Simulate a transient Kafka failure for locationId 1002 only.
        doThrow(new KafkaPublishException("simulated broker timeout"))
                .when(producerService).publish(Mockito.argThat(
                        sr -> sr != null && "1002".equals(sr.getLocationId())));

        MockMultipartFile file = new MockMultipartFile(
                "file", "sample_stores.csv", "text/csv",
                CSV_CONTENT.getBytes(StandardCharsets.UTF_8));

        ImportResult result = csvStoreImportService.importCsv(file);

        // 3 of the 4 valid rows succeed, 1 valid row fails at the Kafka step, plus the
        // 4 originally malformed rows -> 5 total failures, 3 processed.
        assertEquals(8, result.getTotalRows());
        assertEquals(3, result.getProcessedCount());
        assertEquals(5, result.getFailureCount());
        assertTrue(result.getErrors().stream().anyMatch(e -> "kafka".equals(e.getField())));

        // Every valid row was still attempted (4 publish calls total), proving the
        // Kafka failure on one row did not stop the loop from processing the rest.
        verify(producerService, times(4)).publish(any(StoreRecord.class));
    }
}
