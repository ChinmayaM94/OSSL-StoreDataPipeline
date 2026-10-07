package com.safeway.ossl.storepipeline.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Schema(name = "ErrorResponse", description = "Standard API error envelope")
public class ErrorResponse {
    @Schema(example = "2026-10-07T13:30:36.373661Z")
    private String timestamp;

    @Schema(example = "string")
    private int status;

    @Schema(example = "string")
    private String error;

    @Schema(example = "string")
    private String message;
}