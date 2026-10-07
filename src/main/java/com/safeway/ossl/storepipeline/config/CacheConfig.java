package com.safeway.ossl.storepipeline.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * Caffeine in-memory cache configuration for store lookups
 * ({@code GET /api/stores/{storeId}/cache}).
 */
@Configuration
public class CacheConfig {

    @Bean
    public Cache<String, StoreRecord> storeRecordCache() {
        return Caffeine.newBuilder()
                .maximumSize(2000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();
    }
}
