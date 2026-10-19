package com.safeway.ossl.storepipeline.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;

@Schema(description = "Summary + row-level detail of a CSV import operation")
@Data
@Builder
public class ImportResult {

    @Schema(description = "Total number of data rows found in the CSV (excludes the header row).", example = "8")
    private int totalRows;

    @Schema(description = "Number of rows that passed validation and were successfully published to Kafka.",
            example = "4")
    private int processedCount;

    @Schema(description = "Number of rows that failed validation or failed to publish to Kafka.", example = "4")
    private int failureCount;

    @Schema(description = "Row-level detail for every failed row.")
    private List<RowError> errors;

    public ImportResult() {
    }

    public ImportResult(int totalRows, int processedCount, int failureCount, List<RowError> errors) {
        this.totalRows = totalRows;
        this.processedCount = processedCount;
        this.failureCount = failureCount;
        this.errors = errors == null ? Collections.emptyList() : errors;
    }
}
