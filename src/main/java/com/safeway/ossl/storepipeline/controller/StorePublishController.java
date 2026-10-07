package com.safeway.ossl.storepipeline.controller;

import com.safeway.ossl.storepipeline.dto.ErrorResponse;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.exception.StoreNotFoundException;
import com.safeway.ossl.storepipeline.service.StorePublishService;
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
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST endpoint that triggers publication of a store from the SQL datastore
 * to Kafka Topic 1 (ossl-store-publication). This is the entry point to the
 * downstream Mongo-sync + cache-refresh flow.
 */
@RestController
@RequestMapping("/api/stores")
@Tag(name = "Store Publication", description = "Publishes a versioned store record from SQL to Kafka, " +
        "triggering downstream MongoDB sync and cache refresh")
public class StorePublishController {

    @Autowired
    private StorePublishService storePublishService;

    @Operation(
            summary = "Publish a store to Kafka",
            description = "Reads the store identified by storeId from the SQL datastore, assigns it the " +
                    "next version number, persists the new version back to SQL, and publishes the versioned " +
                    "record to the 'ossl-store-publication' Kafka topic. A downstream consumer then upserts " +
                    "the record into MongoDB (only if the incoming version is newer than what's stored) and " +
                    "publishes a change event that evicts the cache entry for this store."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Store published successfully.",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = StoreRecord.class))),
            @ApiResponse(responseCode = "404", description = "No store with this storeId exists in the SQL database.",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Unexpected server error (e.g. Kafka publish failure).",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = ErrorResponse.class)))
    })
    @PostMapping(path = "/{storeId}/publish")
    public ResponseEntity<StoreRecord> publish(
            @Parameter(description = "Store identifier to publish.", required = true)
            @PathVariable String storeId) {

        return ResponseEntity.ok(storePublishService.publish(storeId));
    }
}
