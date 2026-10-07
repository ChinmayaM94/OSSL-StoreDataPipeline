package com.safeway.ossl.storepipeline.exception;

/**
 * Wraps any failure encountered while publishing a single row's
 * {@code StoreRecord} to Kafka (timeout, serialization error, broker
 * unavailable, etc). Caught per-row by {@code CsvStoreImportService} and
 * converted into a {@code RowError} rather than aborting the whole import.
 */
public class KafkaPublishException extends RuntimeException {

    public KafkaPublishException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaPublishException(String message) {
        super(message);
    }
}
