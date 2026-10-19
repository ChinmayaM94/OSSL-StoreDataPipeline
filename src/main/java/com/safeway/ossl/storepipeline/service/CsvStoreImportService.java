package com.safeway.ossl.storepipeline.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.safeway.ossl.storepipeline.dto.ImportResult;
import com.safeway.ossl.storepipeline.dto.RowError;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.exception.CsvHeaderException;
import com.safeway.ossl.storepipeline.exception.EmptyFileException;
import com.safeway.ossl.storepipeline.exception.InvalidFileTypeException;
import com.safeway.ossl.storepipeline.exception.KafkaPublishException;
import com.safeway.ossl.storepipeline.repository.StoreSqlRepository;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.safeway.ossl.storepipeline.constants.CsvStoreFields.REQUIRED_HEADERS;

@Service
public class CsvStoreImportService {

    @Autowired
    private StoreEventProducerService producer;

    @Autowired
    private StoreSqlRepository sqlRepository;

    public ImportResult importCsv(MultipartFile file) {
        validateFile(file);
        List<RowError> errors = new ArrayList<>();
        Set<String> seenLocationIds = new HashSet<>();
        int rows = 0, processed = 0;

        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setIgnoreSurroundingSpaces(true)
                .setTrim(true)
                .setIgnoreEmptyLines(true)
                .build();

        try (CSVParser parser = format.parse(new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8))) {
            List<String> headers = parser.getHeaderNames();
            Set<String> missing = new LinkedHashSet<>(REQUIRED_HEADERS);
            missing.removeAll(headers);
            if (!missing.isEmpty()) {
                throw new CsvHeaderException("CSV is missing required columns: " + missing + "; found: " + headers);
            }

            for (CSVRecord row : parser) {
                rows++;
                int rowNumber = Math.toIntExact(row.getRecordNumber() + 1);
                try {
                    if (!row.isConsistent()) {
                        throw new RowIssue("csv", "Expected " + headers.size() + " values, got " + row.size());
                    }

                    ObjectNode payload = JsonNodeFactory.instance.objectNode();
                    for (String header : REQUIRED_HEADERS) {
                        String cell = row.get(header) == null ? "" : row.get(header).trim();
                        insert(payload, header, convert(header, cell));
                    }

                    String id = payload.path("locationId").asText("");
                    if (id.isBlank()) {
                        throw new RowIssue("locationId", "locationId is required");
                    }

                    if (!seenLocationIds.add(id)) {
                        throw new RowIssue("locationId", "Duplicate locationId: " + id);
                    }

                    payload.remove("locationId");
                    StoreRecord record = new StoreRecord(id, payload, nextVersion(id));

                    producer.publish(record);
                    processed++;
                } catch (RowIssue ex) {
                    errors.add(new RowError(rowNumber, ex.field, ex.getMessage()));
                } catch (Exception ex) {
                    String field = ex instanceof KafkaPublishException ? "kafka" : "csv";
                    errors.add(new RowError(rowNumber, field,
                            ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage()));
                }
            }
        } catch (CsvHeaderException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new CsvHeaderException("Unable to read/parse CSV file: " + ex.getMessage());
        }

        return new ImportResult(rows, processed, errors.size(), errors);
    }

    /**
     * New stores start at version 0. A store already in SQL gets its current version + 1, so a
     * re-import is accepted by the Mongo consumer (which only applies newer versions) and never
     * moves the SQL version backwards.
     */
    private long nextVersion(String locationId) {
        return sqlRepository.findById(locationId)
                .map(entity -> entity.getVersion() == null ? 1L : entity.getVersion() + 1)
                .orElse(0L);
    }

    private static JsonNode convert(String header, String cell) {
        if (cell == null || cell.isEmpty()) return JsonNodeFactory.instance.nullNode();
        String lower = cell.toLowerCase();
        if (lower.equals("true") || lower.equals("false")) return BooleanNode.valueOf(Boolean.parseBoolean(lower));
        if (header.equals("departments")) {
            ArrayNode arr = JsonNodeFactory.instance.arrayNode();
            for (String part : cell.split(",")) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) arr.add(trimmed);
            }
            return arr;
        }
        if (header.equals("latitude") || header.equals("longitude")) {
            try {
                return JsonNodeFactory.instance.numberNode(new BigDecimal(cell));
            } catch (NumberFormatException ignored) {
                return TextNode.valueOf(cell);
            }
        }
        return TextNode.valueOf(cell);
    }

    private static void validateFile(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            throw new EmptyFileException(
                    "Uploaded file is missing or empty. Attach a non-empty CSV file under the 'file' form field.");
        }
        String filename = file.getOriginalFilename();
        String contentType = file.getContentType();
        boolean looksLikeCsv = (filename != null && filename.toLowerCase().endsWith(".csv"))
                || (contentType != null && (
                contentType.equalsIgnoreCase("text/csv")
                        || contentType.equalsIgnoreCase("application/vnd.ms-excel")
                        || contentType.equalsIgnoreCase("application/csv")
                        || contentType.equalsIgnoreCase(MediaType.TEXT_PLAIN_VALUE)));
        if (!looksLikeCsv) {
            throw new InvalidFileTypeException(
                    "Uploaded file does not appear to be a CSV file. filename=" + filename
                            + ", contentType=" + contentType);
        }
    }

    private static void insert(ObjectNode root, String path, JsonNode value) {
        String[] parts = path.split("\\.");
        ObjectNode current = root;
        for (int i = 0; i < parts.length - 1; i++) {
            JsonNode child = current.get(parts[i]);
            if (child == null) child = current.putObject(parts[i]);
            if (!(child instanceof ObjectNode object)) {
                throw new IllegalArgumentException("Conflict: " + path + " has a non-object parent");
            }
            current = object;
        }
        current.set(parts[parts.length - 1], value);
    }

    private static final class RowIssue extends RuntimeException {
        private final String field;
        private RowIssue(String field, String message) {
            super(message);
            this.field = field;
        }
    }
}