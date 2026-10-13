package com.safeway.ossl.storepipeline.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Field-by-field comparison of a store's SQL record vs its MongoDB record")
public class StoreComparisonResult {

    @Schema(description = "Store identifier being compared.", example = "1001")
    private String storeId;

    @Schema(description = "Field values as read from the SQL (relational) datastore. " +
            "Empty if the store was not found in SQL.")
    private Map<String, Object> sqlValues = new LinkedHashMap<>();

    @Schema(description = "Field values as read from MongoDB. Empty if the store was not found in MongoDB.")
    private Map<String, Object> mongoValues = new LinkedHashMap<>();

    @Schema(description = "Names of fields whose SQL and MongoDB values differ. Also includes the markers " +
            "'__missing_in_sql__' / '__missing_in_mongo__' when the store is absent from one of the two sources.")
    private List<String> mismatchedFields;

    @Schema(description = "True only if the store exists in BOTH SQL and MongoDB and every compared field matches.",
            example = "true")
    private boolean consistent;
}
