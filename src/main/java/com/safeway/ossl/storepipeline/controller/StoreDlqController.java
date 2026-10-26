package com.safeway.ossl.storepipeline.controller;

import com.safeway.ossl.storepipeline.dto.DlqReplayResponse;
import com.safeway.ossl.storepipeline.dto.ErrorResponse;
import com.safeway.ossl.storepipeline.service.DlqReplayService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static com.safeway.ossl.storepipeline.constants.OSSLServiceEndPoints.DLQ_REPLAY_URL;
import static com.safeway.ossl.storepipeline.constants.OSSLServiceEndPoints.OSSL_SERVICE_API_PREFIX;

/**
 * Operational endpoint to retry dead-lettered store events. The replay itself runs in the
 * ossl-store-consumer-service microservice; this controller only forwards the request to it.
 */
@RestController
@RequestMapping(OSSL_SERVICE_API_PREFIX)
@Tag(name = "Store DLQ", description = "Replay dead-lettered store events (delegates to the consumer service)")
public class StoreDlqController {

    private static final Logger log = LoggerFactory.getLogger(StoreDlqController.class);

    @Autowired
    private DlqReplayService dlqReplayService;

    @Operation(
            summary = "Replay the dead-letter topic",
            description = "Calls the consumer service, which reads every message currently in " +
                    "'ossl-store-events.DLQ' and publishes it back to 'ossl-store-events' (same key) so the SQL " +
                    "and Mongo consumers retry it. Returns the number of messages replayed."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Replay completed.",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = DlqReplayResponse.class))),
            @ApiResponse(responseCode = "502", description = "The consumer service returned an error.",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "503", description = "The consumer service could not be reached.",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = ErrorResponse.class)))
    })
    @PostMapping(DLQ_REPLAY_URL)
    public ResponseEntity<DlqReplayResponse> replay() {
        log.info("DLQ replay request received; forwarding to consumer service");
        DlqReplayResponse response = dlqReplayService.replay();
        log.info("DLQ replay completed by consumer service: replayed={}", response.replayed());
        return ResponseEntity.ok(response);
    }
}
