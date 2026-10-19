package com.safeway.ossl.storepipeline.mapper;

import com.fasterxml.jackson.databind.JsonNode;
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
    // StoreRecord <-> StoreEntity (+ AddressEntity)
    // ---------------------------------------------------------------

    public static StoreEntity toEntity(StoreRecord record, StoreEntity existing) {
        StoreEntity entity = existing == null ? new StoreEntity() : existing;
        JsonNode p = record.getStoreInfo();

        entity.setLocationId(record.getLocationId());
        entity.setDepartments(joinDepartments(p.path("departments")));
        entity.setIsEcomStore(boolOrNull(p, "isEcomStore"));
        entity.setTimezone(textOrNull(p, "timezone"));
        entity.setBrandName(textOrNull(p.path("brand"), "name"));
        entity.setDivisionName(textOrNull(p.path("division"), "name"));
        entity.setPhone(textOrNull(p, "phone"));
        entity.setLatitude(numberOrNull(p, "latitude"));
        entity.setLongitude(numberOrNull(p, "longitude"));

        JsonNode ecom = p.path("ecomStore");
        entity.setEcomOperatingStatus(textOrNull(ecom, "operatingStatus"));
        entity.setEcomIsDugStore(boolOrNull(ecom, "isDUGStore"));
        entity.setEcomIsDeliveryStore(boolOrNull(ecom, "isDeliveryStore"));
        JsonNode feat = ecom.path("storeFeatures");
        entity.setEcomFeatIsExpressStore(boolOrNull(feat, "isExpressStore"));
        entity.setEcomFeatIsWgdEnabled(boolOrNull(feat, "isWGDEnabled"));
        entity.setEcomFeatIsFlashDugEnabled(boolOrNull(feat, "isFlashDugEnabled"));
        entity.setEcomFeatIsFlashDeliveryEnabled(boolOrNull(feat, "isFlashDeliveryEnabled"));

        entity.setAddress(toAddressEntity(p.path("address"), entity));

        // Never move the stored version backwards (replayed / out-of-order messages).
        Long current = entity.getVersion();
        Long incoming = record.getVersion();
        if (incoming != null) entity.setVersion(current == null ? incoming : Math.max(current, incoming));
        else if (current == null) entity.setVersion(0L);
        return entity;
    }

    private static AddressEntity toAddressEntity(JsonNode address, StoreEntity owner) {
        AddressEntity entity = owner.getAddress() != null ? owner.getAddress() : new AddressEntity();
        entity.setLocationId(owner.getLocationId());
        entity.setStore(owner);
        entity.setAddressLine1(textOrNull(address, "line1"));
        entity.setAddressCity(textOrNull(address, "city"));
        entity.setAddressState(textOrNull(address, "state"));
        entity.setAddressZipcode(textOrNull(address, "zipcode"));
        return entity;
    }

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

    private static String joinDepartments(JsonNode departments) {
        if (!departments.isArray()) return null;
        StringBuilder sb = new StringBuilder();
        for (JsonNode d : departments) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(d.asText());
        }
        return sb.toString();
    }

    private static String textOrNull(JsonNode parent, String field) {
        JsonNode n = parent.path(field);
        return n.isMissingNode() || n.isNull() ? null : n.asText();
    }

    private static Boolean boolOrNull(JsonNode parent, String field) {
        JsonNode n = parent.path(field);
        return n.isBoolean() ? n.asBoolean() : null;
    }

    private static Double numberOrNull(JsonNode parent, String field) {
        JsonNode n = parent.path(field);
        return n.isNumber() ? n.asDouble() : null;
    }

    private static void putIfNotNull(ObjectNode node, String field, String value) {
        if (value != null) node.put(field, value);
    }

    private static void putBoolIfNotNull(ObjectNode node, String field, Boolean value) {
        if (value != null) node.put(field, value);
    }
}