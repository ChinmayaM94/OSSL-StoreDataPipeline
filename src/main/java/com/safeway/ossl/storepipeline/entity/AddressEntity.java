package com.safeway.ossl.storepipeline.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.MapsId;
import jakarta.persistence.OneToOne;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.domain.Persistable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "STORE_ADDRESS")
public class AddressEntity implements Persistable<String> {

    @Id
    @Column(name = "LOCATION_ID")
    private String locationId;

    @OneToOne(fetch = FetchType.LAZY)
    @MapsId
    @JoinColumn(name = "LOCATION_ID")
    private StoreEntity store;

    @Column(name = "ADDRESS_LINE1")
    private String addressLine1;
    @Column(name = "ADDRESS_CITY")
    private String addressCity;
    @Column(name = "ADDRESS_STATE")
    private String addressState;
    @Column(name = "ADDRESS_ZIPCODE")
    private String addressZipcode;

    /**
     * Assigned (not generated) IDs confuse Hibernate's default new-vs-detached
     * check (id == null) once we set locationId manually before the first
     * save. Persistable + this transient flag makes "new" explicit instead,
     * so cascaded inserts from StoreEntity correctly INSERT instead of
     * mistakenly UPDATE-ing a row that doesn't exist yet.
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