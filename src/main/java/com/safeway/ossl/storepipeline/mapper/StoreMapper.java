package com.safeway.ossl.storepipeline.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.safeway.ossl.storepipeline.document.StoreDocument;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.entity.AddressEntity;
import com.safeway.ossl.storepipeline.entity.StoreEntity;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

public final class StoreMapper {

    private static final ObjectMapper JSON = new ObjectMapper();

    private StoreMapper() { }

    // ---------------------------------------------------------------
    // StoreEntity -> StoreRecord
    // ---------------------------------------------------------------

    public static StoreRecord toRecord(StoreEntity entity) {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();

        if (entity.getDepartments() != null && !entity.getDepartments().isBlank()) {
            ArrayNode arr = payload.putArray("departments");
            Arrays.stream(entity.getDepartments().split(","))
                    .map(String::trim).filter(s -> !s.isEmpty()).forEach(arr::add);
        }

        ObjectNode ecom = payload.putObject("ecomStore");
        putIfNotNull(ecom, "operatingStatus", entity.getEcomOperatingStatus());
        putBoolIfNotNull(ecom, "isDUGStore", entity.getEcomIsDugStore());
        putBoolIfNotNull(ecom, "isDeliveryStore", entity.getEcomIsDeliveryStore());
        ObjectNode feat = ecom.putObject("storeFeatures");
        putBoolIfNotNull(feat, "isExpressStore", entity.getEcomFeatIsExpressStore());
        putBoolIfNotNull(feat, "isWGDEnabled", entity.getEcomFeatIsWgdEnabled());
        putBoolIfNotNull(feat, "isFlashDugEnabled", entity.getEcomFeatIsFlashDugEnabled());
        putBoolIfNotNull(feat, "isFlashDeliveryEnabled", entity.getEcomFeatIsFlashDeliveryEnabled());

        putBoolIfNotNull(payload, "isEcomStore", entity.getIsEcomStore());
        putIfNotNull(payload, "timezone", entity.getTimezone());

        ObjectNode brand = payload.putObject("brand");
        putIfNotNull(brand, "name", entity.getBrandName());
        ObjectNode division = payload.putObject("division");
        putIfNotNull(division, "name", entity.getDivisionName());

        AddressEntity addressEntity = entity.getAddress();
        ObjectNode address = payload.putObject("address");
        if (addressEntity != null) {
            putIfNotNull(address, "line1", addressEntity.getAddressLine1());
            putIfNotNull(address, "city", addressEntity.getAddressCity());
            putIfNotNull(address, "state", addressEntity.getAddressState());
            putIfNotNull(address, "zipcode", addressEntity.getAddressZipcode());
        }

        putIfNotNull(payload, "phone", entity.getPhone());
        if (entity.getLatitude() != null) payload.put("latitude", entity.getLatitude());
        if (entity.getLongitude() != null) payload.put("longitude", entity.getLongitude());

        return new StoreRecord(entity.getLocationId(), payload, entity.getVersion());
    }

    // ---------------------------------------------------------------
    // StoreRecord <-> StoreDocument (Mongo keeps the full nested JSON blob)
    // ---------------------------------------------------------------

    public static StoreRecord toRecord(StoreDocument document) {
        ObjectNode payload = mapToObjectNode(document.getStoreInfo());
        return new StoreRecord(document.getLocationId(), payload, document.getVersion());
    }

    public static StoreDocument toDocument(StoreRecord record) {
        if (record.getLocationId() == null) {
            throw new IllegalArgumentException("Record must contain a locationId");
        }

        StoreDocument document = new StoreDocument();
        document.setId(UUID.randomUUID().toString());
        document.setLocationId(record.getLocationId());
        document.setStoreInfo(objectNodeToMap(record.getStoreInfo()));
        document.setVersion(record.getVersion());
        document.setLastUpdated(Instant.now());
        return document;
    }

    // ---------------------------------------------------------------
    // Helpers - ObjectNode <-> Map conversion for Mongo storage
    // ---------------------------------------------------------------

    private static Map<String, Object> objectNodeToMap(ObjectNode node) {
        if (node == null) {
            return Map.of();
        }
        return JSON.convertValue(node, Map.class);
    }

    private static ObjectNode mapToObjectNode(Map<String, Object> map) {
        if (map == null) {
            return JsonNodeFactory.instance.objectNode();
        }
        return JSON.convertValue(map, ObjectNode.class);
    }

    private static void putIfNotNull(ObjectNode node, String field, String value) {
        if (value != null) node.put(field, value);
    }

    private static void putBoolIfNotNull(ObjectNode node, String field, Boolean value) {
        if (value != null) node.put(field, value);
    }
}