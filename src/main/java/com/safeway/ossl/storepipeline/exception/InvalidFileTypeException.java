package com.safeway.ossl.storepipeline.exception;

/**
 * Thrown when the uploaded file does not look like a CSV file (wrong
 * extension/content-type). Request-level failure (HTTP 400).
 */
public class InvalidFileTypeException extends RuntimeException {

    public InvalidFileTypeException(String message) {
        super(message);
    }
}
