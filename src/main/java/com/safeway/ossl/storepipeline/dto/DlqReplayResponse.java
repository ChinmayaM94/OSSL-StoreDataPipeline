package com.safeway.ossl.storepipeline.dto;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Outcome of a dead-letter-queue replay performed by the consumer service")
public record DlqReplayResponse(
        @Schema(description = "Dead-letter topic that was replayed", example = "ossl-store-events.DLQ") String dlqTopic,
        @Schema(description = "Number of messages republished to the store-events topic", example = "3") int replayed) {
}
