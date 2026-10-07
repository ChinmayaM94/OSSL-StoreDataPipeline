package com.safeway.ossl.storepipeline.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.mapping.Field;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Address {
    private String line1;
    private String city;
    private String state;
    private String zipcode;

    /**
     * Fallback coordinate source: some documents in "grocerystores" don't
     * have top-level latitude/longitude fields at all (confirmed via raw
     * driver dump) and only carry coordinates here, as GeoJSON Points.
     * GeoJsonPoint.getX() = longitude, getY() = latitude.
     */
    @Field("displayCoordinates")
    private GeoJsonPoint displayCoordinates;

    @Field("routableCoordinates")
    private GeoJsonPoint routableCoordinates;
}

