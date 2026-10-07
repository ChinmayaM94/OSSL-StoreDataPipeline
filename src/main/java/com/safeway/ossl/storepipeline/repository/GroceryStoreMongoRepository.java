package com.safeway.ossl.storepipeline.repository;

import com.safeway.ossl.storepipeline.models.GroceryStoreDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface GroceryStoreMongoRepository extends MongoRepository<GroceryStoreDocument, String> {
}