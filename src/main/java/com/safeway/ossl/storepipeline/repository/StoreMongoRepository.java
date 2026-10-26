package com.safeway.ossl.storepipeline.repository;

import com.safeway.ossl.storepipeline.document.StoreDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Spring Data MongoDB repository for the downstream synced copy of OSSL
 * store records, keyed by {@code storeId}. Upserted by
 * the store event consumer (ossl-store-consumer-service).
 */
@Repository
public interface StoreMongoRepository extends MongoRepository<StoreDocument, String> {
    Optional<StoreDocument> findByLocationId(String locationId);
}
