package com.safeway.ossl.storepipeline.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EcomStore {
    private String operatingStatus;
    private Boolean isDUGStore;
    private Boolean isDeliveryStore;
    private StoreFeatures storeFeatures;
}