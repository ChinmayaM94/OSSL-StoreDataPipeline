package com.safeway.ossl.storepipeline.constants;

public class OSSLServiceEndPoints {

    private OSSLServiceEndPoints() {
    }

    public static final String OSSL_SERVICE_API_PREFIX = "/api/stores";
    public static final String CSV_INGESTION_URL = "/import";
    public static final String SQL_KAFKA_PUBLICATION_URL = "/{storeId}/publish";
    public static final String GET_SQL_DATA_URL = "/{storeId}/sql";
    public static final String GET_MONGO_DATA_URL = "/{storeId}/mongo";
    public static final String GET_CACHED_DATA_URL = "/{storeId}/cache";
    public static final String COMPARISON_URL = "/{storeId}/compare";
}
