package com.safeway.ossl.storepipeline.exception;

/**
 * Thrown when the uploaded multipart file is missing or empty.
 * This is a request-level failure (HTTP 400), distinct from per-row CSV
 * validation errors which are reported inside {@code ImportResult} instead
 * of being thrown as exceptions.
 */
public class EmptyFileException extends RuntimeException {

    public EmptyFileException(String message) {
        super(message);
    }
}
