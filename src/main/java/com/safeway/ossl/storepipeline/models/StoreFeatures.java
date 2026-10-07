package com.safeway.ossl.storepipeline.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StoreFeatures {
    private Boolean isExpressStore;
    private Boolean isWGDEnabled;
    private Boolean isFlashDugEnabled;
    private Boolean isFlashDeliveryEnabled;
}