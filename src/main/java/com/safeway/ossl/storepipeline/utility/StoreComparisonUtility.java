package com.safeway.ossl.storepipeline.utility;

import com.fasterxml.jackson.databind.JsonNode;
import com.safeway.ossl.storepipeline.dto.StoreRecord;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/** Helpers for flattening store records into comparable maps and diffing them. */
public final class StoreComparisonUtility {

    private StoreComparisonUtility() {}

    /** Flattens a record's store info into dotted-path keys, plus its version. */
    public static Map<String, Object> fields(StoreRecord record) {
        Map<String, Object> map = new LinkedHashMap<>();
        flatten("", record.getStoreInfo(), map);
        map.put("version", record.getVersion());
        return map;
    }

    /** Returns, in sorted order, the keys whose values differ between the two maps (or exist in only one). */
    public static List<String> differingKeys(Map<String, Object> left, Map<String, Object> right) {
        Set<String> keys = new TreeSet<>(left.keySet());
        keys.addAll(right.keySet());
        return keys.stream().filter(k -> !Objects.equals(left.get(k), right.get(k))).toList();
    }

    private static void flatten(String prefix, JsonNode node, Map<String, Object> result) {
        if (node.isObject() && node.size() > 0) {
            node.fields().forEachRemaining(e ->
                    flatten(prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey(), e.getValue(), result));
        } else {
            result.put(prefix, node);
        }
    }
}
