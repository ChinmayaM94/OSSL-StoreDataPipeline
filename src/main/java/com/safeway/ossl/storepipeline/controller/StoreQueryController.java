package com.safeway.ossl.storepipeline.controller;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.safeway.ossl.storepipeline.dto.ErrorResponse;
import com.safeway.ossl.storepipeline.dto.StoreComparisonResult;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.service.StoreQueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static com.safeway.ossl.storepipeline.constants.OSSLServiceEndPoints.*;

/**
 * Read-only endpoints to inspect a store directly from each layer of the
 * pipeline (SQL, MongoDB, Caffeine cache), plus a consistency-check
 * comparison endpoint — useful for verifying the async publish/sync flow
 * actually converged, per the Confluence LLD "OSSL Store Data Pipeline".
 */
@RestController
@RequestMapping(OSSL_SERVICE_API_PREFIX)
@Tag(name = "Store Queries", description = "Read a store from SQL, MongoDB, or the cache, and compare them for consistency")
public class StoreQueryController {

    private static final Logger log = LoggerFactory.getLogger(StoreQueryController.class);

    @Autowired
    private StoreQueryService storeQueryService;

    @Operation(summary = "Get a store from the SQL database",
            description = "Reads the store directly from the relational (SQL) datastore.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Store found.",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = StoreRecord.class))),
            @ApiResponse(responseCode = "404", description = "No store with this storeId exists in SQL.",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = ErrorResponse.class)))
    })
    @GetMapping(GET_SQL_DATA_URL)
    public ResponseEntity<StoreRecord> getFromSql(
            @Parameter(description = "Store identifier.", required = true, example = "1001")
            @PathVariable String storeId) {
        log.info("GET store from SQL requested: storeId={}", storeId);
        return ResponseEntity.ok(storeQueryService.getFromSql(storeId));
    }

    @Operation(summary = "Get a store from MongoDB",
            description = "Reads the store directly from MongoDB (the datastore synced by the Kafka store-events consumer).")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Store found.",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = StoreRecord.class))),
            @ApiResponse(responseCode = "404", description = "No store with this storeId exists in MongoDB.",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = ErrorResponse.class)))
    })
    @GetMapping(GET_MONGO_DATA_URL)
    public ResponseEntity<StoreRecord> getFromMongo(
            @Parameter(description = "Store identifier.", required = true, example = "1001")
            @PathVariable String storeId) {
        log.info("GET store from MongoDB requested: storeId={}", storeId);
        return ResponseEntity.ok(storeQueryService.getFromMongo(storeId));
    }

    @Operation(summary = "Get a store from the cache (cache-aside)",
            description = "Reads from the Caffeine in-memory cache. On a cache miss, loads from MongoDB and " +
                    "populates the cache before returning (cache-aside pattern).")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Store found (from cache or loaded from MongoDB).",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = StoreRecord.class))),
            @ApiResponse(responseCode = "404", description = "No store with this storeId exists in the cache or MongoDB.",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = ErrorResponse.class)))
    })
    @GetMapping(GET_CACHED_DATA_URL)
    public ResponseEntity<StoreRecord> getFromCache(
            @Parameter(description = "Store identifier.", required = true, example = "1001")
            @PathVariable String storeId) {
        log.info("GET store from cache requested: storeId={}", storeId);
        return ResponseEntity.ok(storeQueryService.getFromCache(storeId));
    }

    @Operation(summary = "Compare a store's SQL and MongoDB copies",
            description = "Fetches the store from both SQL and MongoDB and returns a field-by-field diff — " +
                    "useful for verifying the async publish -> Kafka -> Mongo-sync pipeline has converged.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Comparison result (may report consistent=false).",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = StoreComparisonResult.class))),
            @ApiResponse(responseCode = "404", description = "Store not found in SQL or MongoDB.",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = ErrorResponse.class)))
    })
    @GetMapping(COMPARISON_URL)
    public ResponseEntity<StoreComparisonResult> compare(
            @Parameter(description = "Store identifier.", required = true, example = "1001")
            @PathVariable String storeId) {
        log.info("Store comparison requested: storeId={}", storeId);
        return ResponseEntity.ok(storeQueryService.compare(storeId));
    }
}
