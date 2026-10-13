package com.safeway.ossl.storepipeline.service;

import com.safeway.ossl.storepipeline.dto.ImportResult;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.exception.KafkaPublishException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.safeway.ossl.storepipeline.constants.CsvStoreFields.REQUIRED_HEADERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class CsvStoreImportServiceTest {

    private static final String CSV_CONTENT = String.join("\n",
            String.join(",", REQUIRED_HEADERS),
            row("1001", "47.6097", "-122.3331"),
            row("1002", "47.6101", "-122.2015"),
            row("1003", "45.5231", "-122.6765"),
            row("", "47.2529", "-122.4443"),
            row("1002", "47.6588", "-117.4260"),
            "1006,grocery,ACTIVE",
            row("1007", "47.0379", "-120.5059") + ",EXTRA",
            row("1008", "46.6021", "-120.5059")
    ) + "\n";

    private StoreEventProducerService producerService;
    private CsvStoreImportService csvStoreImportService;

    @BeforeEach
    void setUp() {
        producerService = Mockito.mock(StoreEventProducerService.class);
        csvStoreImportService = new CsvStoreImportService();
        ReflectionTestUtils.setField(csvStoreImportService, "producer", producerService);
    }

    @Test
    void mixedValidAndMalformedRows_producesCorrectCountsAndDoesNotThrow() throws IOException {
        MockMultipartFile file = new MockMultipartFile(
                "file", "sample_stores.csv", "text/csv",
                CSV_CONTENT.getBytes(StandardCharsets.UTF_8));

        ImportResult result = csvStoreImportService.importCsv(file);

        assertEquals(8, result.getTotalRows());
        assertEquals(4, result.getProcessedCount());
        assertEquals(4, result.getFailureCount());
        assertEquals(4, result.getErrors().size());

        verify(producerService, times(4)).publish(any(StoreRecord.class));

        ArgumentCaptor<StoreRecord> captor = ArgumentCaptor.forClass(StoreRecord.class);
        verify(producerService, times(4)).publish(captor.capture());
        List<String> publishedIds = captor.getAllValues().stream().map(StoreRecord::getLocationId).toList();
        assertTrue(publishedIds.containsAll(List.of("1001", "1002", "1003", "1008")));
    }

    @Test
    void kafkaPublishFailureOnOneRow_isCountedAsFailureAndOtherRowsStillPublish() throws IOException {
        doThrow(new KafkaPublishException("simulated broker timeout"))
                .when(producerService).publish(Mockito.argThat(
                        sr -> sr != null && "1002".equals(sr.getLocationId())));

        MockMultipartFile file = new MockMultipartFile(
                "file", "sample_stores.csv", "text/csv",
                CSV_CONTENT.getBytes(StandardCharsets.UTF_8));

        ImportResult result = csvStoreImportService.importCsv(file);

        assertEquals(8, result.getTotalRows());
        assertEquals(3, result.getProcessedCount());
        assertEquals(5, result.getFailureCount());
        assertTrue(result.getErrors().stream().anyMatch(e -> "kafka".equals(e.getField())));

        verify(producerService, times(4)).publish(any(StoreRecord.class));
    }

    private static String row(String locationId, String latitude, String longitude) {
        List<String> values = new ArrayList<>();
        values.add(locationId);
        values.add("grocery");
        values.add("ACTIVE");
        values.add("true");
        values.add("false");
        values.add("true");
        values.add("false");
        values.add("true");
        values.add("false");
        values.add("true");
        values.add("PST");
        values.add("Safeway");
        values.add("Northwest");
        values.add("123 Main");
        values.add("Seattle");
        values.add("WA");
        values.add("98101");
        values.add("555-1111");
        values.add(latitude);
        values.add(longitude);
        return String.join(",", values);
    }
}