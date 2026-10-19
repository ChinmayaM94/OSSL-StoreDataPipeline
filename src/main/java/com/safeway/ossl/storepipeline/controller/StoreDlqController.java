package com.safeway.ossl.storepipeline.controller;

import com.safeway.ossl.storepipeline.service.DlqReplayService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.safeway.ossl.storepipeline.constants.OSSLServiceEndPoints.DLQ_REPLAY_URL;
import static com.safeway.ossl.storepipeline.constants.OSSLServiceEndPoints.OSSL_SERVICE_API_PREFIX;

/**
 * Operational endpoint to retry messages that ended up in the dead-letter topic.
 */
@RestController
@RequestMapping(OSSL_SERVICE_API_PREFIX)
@Tag(name = "Store DLQ", description = "Replay dead-lettered store events")
public class StoreDlqController {

    @Autowired
    private DlqReplayService dlqReplayService;

    @Operation(
            summary = "Replay the dead-letter topic",
            description = "Reads every message currently in 'ossl-store-events.DLQ' and publishes it back to " +
                    "'ossl-store-events' (same key), so the SQL and Mongo consumers retry it. Call this after " +
                    "fixing the cause of the failure (e.g. MongoDB is back up). Messages that fail again are " +
                    "retried and dead-lettered again. Returns the number of messages replayed."
    )
    @PostMapping(DLQ_REPLAY_URL)
    public ResponseEntity<Map<String, Object>> replay() {
        int replayed = dlqReplayService.replay();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("dlqTopic", dlqReplayService.getDlqTopic());
        body.put("replayed", replayed);
        return ResponseEntity.ok(body);
    }
}
