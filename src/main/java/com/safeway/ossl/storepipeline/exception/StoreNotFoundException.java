package com.safeway.ossl.storepipeline.exception;

/**
 * Thrown when a requested store cannot be found in the datastore(s) being
 * queried (SQL, MongoDB, cache, or both for /compare). Handled by
 * {@link GlobalExceptionHandler} and returned as HTTP 404.
 */
public class StoreNotFoundException extends RuntimeException {

    public StoreNotFoundException(String message) {
        super(message);
    }

    /** Builds the standard "Store 'id' was not found in source." message. */
    public static StoreNotFoundException forStore(String storeId, String source) {
        return new StoreNotFoundException("Store '" + storeId + "' was not found in " + source + ".");
    }
}
