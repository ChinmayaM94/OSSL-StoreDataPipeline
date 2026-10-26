package com.safeway.ossl.storepipeline.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
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
    public Cache<String, StoreRecord> storeRecordCache(MeterRegistry meterRegistry) {
        Cache<String, StoreRecord> cache = Caffeine.newBuilder()
                .maximumSize(2500)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .recordStats()
                .build();
        return CaffeineCacheMetrics.monitor(meterRegistry, cache, "storeRecordCache");
    }
}
