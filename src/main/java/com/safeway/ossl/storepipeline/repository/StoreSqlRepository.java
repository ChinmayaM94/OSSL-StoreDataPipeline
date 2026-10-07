package com.safeway.ossl.storepipeline.repository;

import com.safeway.ossl.storepipeline.entity.StoreEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Spring Data JPA repository for the relational ("SQL") copy of OSSL store records.
 */
@Repository
public interface StoreSqlRepository extends JpaRepository<StoreEntity, String> {
}
