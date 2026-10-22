package com.safeway.ossl.storepipeline.utility;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.safeway.ossl.storepipeline.exception.KafkaPublishException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Shared handling for blocking on a Kafka send, so every producer applies the same timeout and error mapping. */
public final class KafkaPublishUtility {

    private static final Logger log = LoggerFactory.getLogger(KafkaPublishUtility.class);

    public static final long PUBLISH_TIMEOUT_SECONDS = 5;

    private KafkaPublishUtility() {}

    public static <T> T awaitSend(CompletableFuture<T> future, String action) {
        try {
            return future.get(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted while trying to {}", action);
            Thread.currentThread().interrupt();
            throw new KafkaPublishException("Interrupted while trying to " + action, e);
        } catch (ExecutionException | TimeoutException e) {
            log.error("Kafka send failed, unable to {}: {}", action, e.toString());
            throw new KafkaPublishException("Failed to " + action, e);
        }
    }
}
