package com.safeway.ossl.storepipeline.repository;

import com.safeway.ossl.storepipeline.document.StoreDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

/**
 * Spring Data MongoDB repository for the downstream synced copy of OSSL
 * store records, keyed by {@code storeId}. Upserted by
 * {@link com.safeway.ossl.storepipeline.listener.StorePublicationConsumer}.
 */
@Repository
public interface StoreMongoRepository extends MongoRepository<StoreDocument, String> {
}
