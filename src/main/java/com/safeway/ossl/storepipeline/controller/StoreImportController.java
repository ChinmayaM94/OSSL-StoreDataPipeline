package com.safeway.ossl.storepipeline.controller;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.safeway.ossl.storepipeline.dto.ErrorResponse;
import com.safeway.ossl.storepipeline.dto.ImportResult;
import com.safeway.ossl.storepipeline.service.CsvStoreImportService;
import com.safeway.ossl.storepipeline.validation.ValidCsvFile;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import static com.safeway.ossl.storepipeline.constants.OSSLServiceEndPoints.CSV_INGESTION_URL;
import static com.safeway.ossl.storepipeline.constants.OSSLServiceEndPoints.OSSL_SERVICE_API_PREFIX;

/**
 * REST endpoint for importing OSSL store CSV files.
 */
@RestController
@RequestMapping(OSSL_SERVICE_API_PREFIX)
@Tag(name = "Store Import", description = "OSSL store CSV import and Kafka publishing")
public class StoreImportController {

    private static final Logger log = LoggerFactory.getLogger(StoreImportController.class);

    private final CsvStoreImportService csvStoreImportService;

    public StoreImportController(CsvStoreImportService csvStoreImportService) {
        this.csvStoreImportService = csvStoreImportService;
    }

    @Operation(
            summary = "Import an OSSL store CSV file",
            description = "Load the OSSL store CSV data into the SQL database. CSV uploads also produce store update events."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Import completed. Inspect processedCount/" +
                    "failureCount and the errors list for the row-level outcome.",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ImportResult.class))),
            @ApiResponse(responseCode = "400", description = "File missing/empty, not a CSV, or the CSV " +
                    "header is missing required columns.",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Unexpected server error.",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorResponse.class)))
    })
    @PostMapping(path = CSV_INGESTION_URL, consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<ImportResult> importStores(
            @Parameter(description = "CSV file containing OSSL store records",
                    required = true, content = @Content(mediaType = MediaType.MULTIPART_FORM_DATA_VALUE))
            @RequestParam("file") @Valid @ValidCsvFile MultipartFile file) {

        log.info("CSV import request received: filename={} size={} bytes", file.getOriginalFilename(), file.getSize());
        ImportResult result = csvStoreImportService.importCsv(file);
        log.info("CSV import request completed: filename={} processed={} failed={}",
                file.getOriginalFilename(), result.getProcessedCount(), result.getFailureCount());
        return ResponseEntity.ok(result);
    }
}
