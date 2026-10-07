package com.safeway.ossl.storepipeline.exception;

/**
 * Thrown when the CSV file cannot be parsed at all, or its header row is
 * missing the minimum required columns (storeId, storeName). This is a
 * whole-file, request-level failure (HTTP 400) — distinct from individual
 * malformed data rows, which are captured as {@code RowError}s instead of
 * aborting the request.
 */
public class CsvHeaderException extends RuntimeException {

    public CsvHeaderException(String message) {
        super(message);
    }
}
