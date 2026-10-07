package com.safeway.ossl.storepipeline.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.domain.Persistable;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "STORES")
public class StoreEntity implements Persistable<String> {
    @Id
    @Column(name = "LOCATION_ID", nullable = false, updatable = false)
    private String locationId;

    @Column(name = "DEPARTMENTS", length = 1000)
    private String departments;

    @Column(name = "OPERATING_STATUS")
    private String ecomOperatingStatus;

    @Column(name = "IS_DUG_STORE")
    private Boolean ecomIsDugStore;

    @Column(name = "IS_DELIVERY_STORE")
    private Boolean ecomIsDeliveryStore;

    @Column(name = "IS_EXPRESS_STORE")
    private Boolean ecomFeatIsExpressStore;

    @Column(name = "IS_WGD_ENABLED")
    private Boolean ecomFeatIsWgdEnabled;

    @Column(name = "IS_FLASH_DUG_ENABLED")
    private Boolean ecomFeatIsFlashDugEnabled;

    @Column(name = "IS_FLASH_DELIVERY_ENABLED")
    private Boolean ecomFeatIsFlashDeliveryEnabled;

    @Column(name = "IS_ECOM_STORE")
    private Boolean isEcomStore;

    @Column(name = "TIMEZONE")
    private String timezone;

    @Column(name = "BRAND_NAME")
    private String brandName;

    @Column(name = "DIVISION_NAME")
    private String divisionName;

    @OneToOne(mappedBy = "store", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    private AddressEntity address;

    @Column(name = "PHONE")
    private String phone;

    @Column(name = "LATITUDE")
    private Double latitude;

    @Column(name = "LONGITUDE")
    private Double longitude;

    @Column(name = "RECORD_VERSION")
    private Long version;

    /**
     * Assigned (not generated) ID means Spring Data's default new-vs-detached
     * check (id == null) always sees a non-null locationId and assumes the
     * entity already exists, causing save() to call merge() instead of
     * persist() for brand-new stores. That wrongly cascades MERGE (not
     * PERSIST) onto new child entities like AddressEntity, which then try
     * to UPDATE a row that doesn't exist yet. Persistable makes "new" explicit.
     */
    @Transient
    private boolean isNew = true;

    @Override
    public String getId() {
        return locationId;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    @PostLoad
    @PostPersist
    void markNotNew() {
        this.isNew = false;
    }
}