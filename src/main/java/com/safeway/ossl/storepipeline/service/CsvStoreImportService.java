package com.safeway.ossl.storepipeline.service;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.safeway.ossl.storepipeline.dto.ImportResult;
import com.safeway.ossl.storepipeline.dto.RowError;
import com.safeway.ossl.storepipeline.exception.CsvHeaderException;
import com.safeway.ossl.storepipeline.utility.CsvStoreImportUtility;
import com.safeway.ossl.storepipeline.utility.ExceptionUtility;
import com.safeway.ossl.storepipeline.utility.StoreBatchPublishUtility;
import com.safeway.ossl.storepipeline.utility.StoreBatchPublishUtility.PendingRow;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
public class CsvStoreImportService {

    private static final Logger log = LoggerFactory.getLogger(CsvStoreImportService.class);

    private final StoreBatchPublishUtility batchPublisher;

    public CsvStoreImportService(StoreBatchPublishUtility batchPublisher) {
        this.batchPublisher = batchPublisher;
    }

    public ImportResult importCsv(MultipartFile file) {
        List<RowError> errors = new ArrayList<>();
        Set<String> seenLocationIds = new HashSet<>();
        List<PendingRow> pending = new ArrayList<>();
        int rows = 0;

        log.info("Starting CSV import: filename={} size={} bytes", file.getOriginalFilename(), file.getSize());
        try (CSVParser parser = CsvStoreImportUtility.CSV_FORMAT.parse(
                new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8))) {
            List<String> headers = parser.getHeaderNames();
            List<String> missing = CsvStoreImportUtility.missingHeaders(headers);
            if (!missing.isEmpty()) {
                log.warn("CSV import rejected: missing required columns {}", missing);
                throw new CsvHeaderException("CSV is missing required columns: " + missing + "; found: " + headers);
            }

            for (CSVRecord row : parser) {
                rows++;
                int rowNumber = Math.toIntExact(row.getRecordNumber() + 1);

                try {
                    if (!row.isConsistent()) {
                        throw new RowIssue("csv", "Expected " + headers.size() + " values, got " + row.size());
                    }

                    ObjectNode payload = CsvStoreImportUtility.toPayload(row);

                    String id = payload.path("locationId").asText("");
                    if (id.isBlank()) {
                        throw new RowIssue("locationId", "locationId is required");
                    }

                    if (!seenLocationIds.add(id)) {
                        throw new RowIssue("locationId", "Duplicate locationId: " + id);
                    }

                    payload.remove("locationId");
                    pending.add(new PendingRow(rowNumber, id, payload));
                } catch (RowIssue ex) {
                    log.debug("CSV row {} rejected: field={} reason={}", rowNumber, ex.field, ex.getMessage());
                    errors.add(new RowError(rowNumber, ex.field, ex.getMessage()));
                } catch (Exception ex) {
                    log.warn("CSV row {} could not be parsed: {}", rowNumber, ExceptionUtility.errorMessage(ex));
                    errors.add(new RowError(rowNumber, "csv", ExceptionUtility.errorMessage(ex)));
                }
            }
        } catch (CsvHeaderException ex) {
            throw ex;
        } catch (Exception ex) {
            log.error("Unable to read/parse CSV file {}", file.getOriginalFilename(), ex);
            throw new CsvHeaderException("Unable to read/parse CSV file: " + ex.getMessage());
        }

        log.info("CSV parsing finished: rows={} valid={} rejected={}", rows, pending.size(), errors.size());
        int processed = batchPublisher.publishAll(pending, errors);
        errors.sort(Comparator.comparingInt(RowError::getRowNumber));
        log.info("CSV import finished: rows={} processed={} failed={}", rows, processed, errors.size());
        return new ImportResult(rows, processed, errors.size(), errors);
    }

    private static final class RowIssue extends RuntimeException {
        private final String field;

        private RowIssue(String field, String message) {
            super(message);
            this.field = field;
        }
    }
}