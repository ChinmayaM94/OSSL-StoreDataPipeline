package com.safeway.ossl.storepipeline.exception;

import org.springframework.http.HttpStatus;

/**
 * A call to the ossl-store-consumer-service failed: either it could not be reached
 * ({@code 503}) or it answered with an error ({@code 502}).
 */
public class ConsumerServiceException extends RuntimeException {

    private final HttpStatus status;

    public ConsumerServiceException(HttpStatus status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    public HttpStatus getStatus() {
        return status;
    }
}
