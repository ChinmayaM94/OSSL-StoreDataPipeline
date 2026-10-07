package com.safeway.ossl.storepipeline.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Schema(description = "A single row-level validation or publish failure")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RowError {

    @Schema(description = "1-based row number in the CSV file, counting the header as row 1 " +
            "(so the first data row is row 2).", example = "4")
    private int rowNumber;

    @Schema(description = "Comma-separated name(s) of the offending field(s), or 'kafka'/'unknown' " +
            "for non-validation failures.", example = "storeId")
    private String field;

    @Schema(description = "Human-readable, actionable reason the row failed.",
            example = "storeId is required and cannot be blank")
    private String reason;
}
