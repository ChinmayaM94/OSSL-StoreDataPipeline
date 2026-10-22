package com.safeway.ossl.storepipeline.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.multipart.MultipartException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Handles request-level failures only (bad/missing/oversized file, wrong
 * content type, unexpected server errors). Per-row CSV validation issues are
 * NOT exceptions — they are collected as {@code RowError} entries inside the
 * {@code ImportResult} response, per Jira ACIP-624075 AC #4.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(EmptyFileException.class)
    public ResponseEntity<Map<String, Object>> handleEmptyFile(EmptyFileException ex) {
        log.warn("Empty/missing file upload: {}", ex.getMessage());
        return build(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    @ExceptionHandler(InvalidFileTypeException.class)
    public ResponseEntity<Map<String, Object>> handleInvalidFileType(InvalidFileTypeException ex) {
        log.warn("Invalid file type uploaded: {}", ex.getMessage());
        return build(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    @ExceptionHandler(HandlerMethodValidationException.class)
    public ResponseEntity<Map<String, Object>> handleMethodValidation(HandlerMethodValidationException ex) {
        String message = ex.getAllErrors().stream()
                .map(MessageSourceResolvable::getDefaultMessage)
                .filter(Objects::nonNull)
                .collect(Collectors.joining("; "));
        log.warn("Request validation failed: {}", message);
        return build(HttpStatus.BAD_REQUEST, message.isEmpty() ? "Request validation failed." : message);
    }

    @ExceptionHandler(CsvHeaderException.class)
    public ResponseEntity<Map<String, Object>> handleCsvHeader(CsvHeaderException ex) {
        log.warn("CSV could not be parsed / header invalid: {}", ex.getMessage());
        return build(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    @ExceptionHandler(StoreNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleStoreNotFound(StoreNotFoundException ex) {
        log.warn("Store not found: {}", ex.getMessage());
        return build(HttpStatus.NOT_FOUND, ex.getMessage());
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<Map<String, Object>> handleMissingParam(MissingServletRequestParameterException ex) {
        log.warn("Missing required request parameter: {}", ex.getMessage());
        return build(HttpStatus.BAD_REQUEST, "Missing required parameter: " + ex.getParameterName());
    }

    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public ResponseEntity<Map<String, Object>> handleMaxUploadSize(MaxUploadSizeExceededException ex) {
        log.warn("Uploaded file exceeded max size: {}", ex.getMessage());
        return build(HttpStatus.BAD_REQUEST, "Uploaded file is too large. Maximum allowed size is 10MB.");
    }

    @ExceptionHandler(MultipartException.class)
    public ResponseEntity<Map<String, Object>> handleMultipart(MultipartException ex) {
        log.warn("Malformed multipart request: {}", ex.getMessage());
        return build(HttpStatus.BAD_REQUEST, "Malformed multipart request: " + ex.getMessage());
    }

    /**
     * A backing data store (MongoDB / PostgreSQL) cannot be reached. Reported as 503 with a short
     * message instead of leaking the driver's connection details; the full exception is logged.
     */
    @ExceptionHandler({DataAccessResourceFailureException.class, CannotCreateTransactionException.class})
    public ResponseEntity<Map<String, Object>> handleDataStoreUnavailable(Exception ex) {
        log.error("Data store unavailable", ex);
        return build(HttpStatus.SERVICE_UNAVAILABLE, describeUnavailableStore(ex) + " is currently unavailable. Please try again shortly.");
    }

    private static String describeUnavailableStore(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            String name = t.getClass().getName();
            if (name.startsWith("com.mongodb")) return "MongoDB";
            if (name.startsWith("org.postgresql") || name.startsWith("org.hibernate")
                    || name.startsWith("java.sql") || name.startsWith("com.zaxxer")) return "The SQL database";
        }
        return "A data store";
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleUnexpected(Exception ex) {
        log.error("Unexpected server error", ex);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "Unexpected server error: " + ex.getMessage());
    }

    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<Map<String, Object>> handleNoResourceFound(NoResourceFoundException ex) {
        log.debug("Missing static resource requested: {}", ex.getMessage());
        return ResponseEntity.notFound().build();
    }

    private ResponseEntity<Map<String, Object>> build(HttpStatus status, String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", Instant.now().toString());
        body.put("status", status.value());
        body.put("error", status.getReasonPhrase());
        body.put("message", message);
        return ResponseEntity.status(status).body(body);
    }
}
