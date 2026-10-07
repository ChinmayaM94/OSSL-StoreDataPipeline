package com.safeway.ossl.storepipeline.constants;

import java.util.List;

public final class CsvStoreFields {

    private CsvStoreFields() {
    }

    /** Fixed set of columns the uploaded CSV must contain. No value-level schema validation is applied. */
    public static final List<String> REQUIRED_HEADERS = List.of(
            "locationId",
            "departments",
            "ecomStore.operatingStatus",
            "ecomStore.isDUGStore",
            "ecomStore.isDeliveryStore",
            "ecomStore.storeFeatures.isExpressStore",
            "ecomStore.storeFeatures.isWGDEnabled",
            "ecomStore.storeFeatures.isFlashDugEnabled",
            "ecomStore.storeFeatures.isFlashDeliveryEnabled",
            "isEcomStore",
            "timezone",
            "brand.name",
            "division.name",
            "address.line1",
            "address.city",
            "address.state",
            "address.zipcode",
            "phone",
            "latitude",
            "longitude"
    );
}