package com.safeway.ossl.storepipeline.utility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.safeway.ossl.storepipeline.constants.CsvStoreFields;
import com.safeway.ossl.storepipeline.models.GroceryStoreDocument;
import com.safeway.ossl.storepipeline.repository.GroceryStoreMongoRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


@Slf4j
@Component
@RequiredArgsConstructor
public class StoreCsvExportUtility {

    private final GroceryStoreMongoRepository storeRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** Fixed set of columns to export, in order - single source of truth shared with CSV import validation. */
    private static final List<String> HEADERS = CsvStoreFields.REQUIRED_HEADERS;

    public Path exportStoresToCsv(String fileName) throws IOException {
        List<GroceryStoreDocument> stores = storeRepository.findAll();
        log.info("Fetched {} stores from grocerystores collection", stores.size());

        Path outputPath = Paths.get(System.getProperty("user.dir"), "sample-data", fileName);
        java.nio.file.Files.createDirectories(outputPath.getParent());

        try (CSVPrinter printer = new CSVPrinter(
                java.nio.file.Files.newBufferedWriter(outputPath, java.nio.charset.StandardCharsets.UTF_8),
                CSVFormat.DEFAULT.builder().setHeader(HEADERS.toArray(new String[0])).build())) {

            for (GroceryStoreDocument store : stores) {
                JsonNode node = objectMapper.valueToTree(store);
                List<String> values = new ArrayList<>(HEADERS.size());
                for (String header : HEADERS) {
                    if ("latitude".equals(header)) {
                        values.add(resolveCoordinate(store, true));
                    } else if ("longitude".equals(header)) {
                        values.add(resolveCoordinate(store, false));
                    } else {
                        values.add(resolveValue(node, header));
                    }
                }
                printer.printRecord(values);
            }
        }

        log.info("Wrote {} store records with {} columns to {}", stores.size(), HEADERS.size(),
                outputPath.toAbsolutePath());
        return outputPath;
    }

    private String resolveCoordinate(GroceryStoreDocument store, boolean wantLatitude) {
        Double direct = wantLatitude ? store.getLatitude() : store.getLongitude();
        if (direct != null) return direct.toString();

        if (store.getAddress() != null) {
            org.springframework.data.mongodb.core.geo.GeoJsonPoint point = store.getAddress().getDisplayCoordinates();
            if (point == null) point = store.getAddress().getRoutableCoordinates();
            if (point != null) {
                return String.valueOf(wantLatitude ? point.getY() : point.getX());
            }
        }
        return "";
    }

    private String resolveValue(JsonNode root, String header) {
        if ("departments".equals(header)) {
            JsonNode departments = root.get("departments");
            if (departments == null || !departments.isArray() || departments.isEmpty()) {
                return "";
            }
            StringBuilder joined = new StringBuilder();
            Iterator<JsonNode> it = departments.elements();
            while (it.hasNext()) {
                joined.append(it.next().asText());
                if (it.hasNext()) {
                    joined.append(", ");
                }
            }
            return joined.toString();
        }

        JsonNode current = root;
        for (String part : header.split("\\.")) {
            if (current == null) {
                return "";
            }
            current = current.get(part);
        }
        return (current == null || current.isNull()) ? "" : current.asText();
    }
}