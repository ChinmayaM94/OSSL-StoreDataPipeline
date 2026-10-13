package com.safeway.ossl.storepipeline.repository;

import com.safeway.ossl.storepipeline.document.StoreDocument;
import com.safeway.ossl.storepipeline.listener.StoreEventConsumer;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Spring Data MongoDB repository for the downstream synced copy of OSSL
 * store records, keyed by {@code storeId}. Upserted by
 * {@link StoreEventConsumer}.
 */
@Repository
public interface StoreMongoRepository extends MongoRepository<StoreDocument, String> {
    Optional<StoreDocument> findByLocationId(String locationId);
}
