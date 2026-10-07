package com.safeway.ossl.storepipeline.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "grocerystores")
public class GroceryStoreDocument {

    @Id
    private String id;

    @Field("locationId")
    private String locationId;

    private List<String> departments;

    private EcomStore ecomStore;

    private Boolean isEcomStore;

    private String timezone;

    private Brand brand;

    private Division division;

    private Address address;

    private String phone;

    private Double latitude;

    private Double longitude;
}