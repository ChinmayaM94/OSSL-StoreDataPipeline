package com.safeway.ossl.storepipeline.utility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.safeway.ossl.storepipeline.constants.CsvStoreFields.REQUIRED_HEADERS;

public final class CsvStoreImportUtility {

    /** Immutable and thread-safe, so built once and shared. */
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .setIgnoreSurroundingSpaces(true)
            .setTrim(true)
            .setIgnoreEmptyLines(true)
            .build();

    /** Dotted header paths split once instead of on every cell of every row. */
    private static final Map<String, String[]> PATHS = REQUIRED_HEADERS.stream()
            .collect(Collectors.toUnmodifiableMap(Function.identity(), h -> h.split("\\.")));

    private CsvStoreImportUtility() {}

    /** Builds the nested JSON payload for one CSV row from the required headers (includes locationId). */
    public static ObjectNode toPayload(CSVRecord row) {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        for (String header : REQUIRED_HEADERS) {
            String cell = row.get(header); // CSVFormat trim already applied
            insert(payload, header, convert(header, cell));
        }
        return payload;
    }

    /** Returns the required headers absent from {@code headers}, in required order. */
    public static List<String> missingHeaders(List<String> headers) {
        return REQUIRED_HEADERS.stream().filter(h -> !headers.contains(h)).toList();
    }

    public static JsonNode convert(String header, String cell) {
        if (cell == null || cell.isEmpty()) return JsonNodeFactory.instance.nullNode();
        String lower = cell.toLowerCase();
        if (lower.equals("true") || lower.equals("false")) {
            return BooleanNode.valueOf(Boolean.parseBoolean(lower));
        }
        if ("departments".equals(header)) {
            ArrayNode arr = JsonNodeFactory.instance.arrayNode();
            for (String part : cell.split(",")) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) arr.add(trimmed);
            }
            return arr;
        }
        if ("latitude".equals(header) || "longitude".equals(header)) {
            try {
                return JsonNodeFactory.instance.numberNode(new BigDecimal(cell));
            } catch (NumberFormatException ignored) {
                return TextNode.valueOf(cell);
            }
        }
        return TextNode.valueOf(cell);
    }

    public static void insert(ObjectNode root, String path, JsonNode value) {
        String[] parts = PATHS.getOrDefault(path, path.split("\\."));
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
}