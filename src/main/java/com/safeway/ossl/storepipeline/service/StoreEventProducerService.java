package com.safeway.ossl.storepipeline.service;

import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.exception.KafkaPublishException;
import com.safeway.ossl.storepipeline.utility.KafkaPublishUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class StoreEventProducerService {

    private static final Logger log = LoggerFactory.getLogger(StoreEventProducerService.class);

    private final KafkaTemplate<String, StoreRecord> kafkaTemplate;
    private final String topic;

    public StoreEventProducerService(KafkaTemplate<String, StoreRecord> storePayloadKafkaTemplate,
                                      @Value("${kafka.topic.store-events}") String topic) {
        this.kafkaTemplate = storePayloadKafkaTemplate;
        this.topic = topic;
    }

    /**
     * Sends without waiting for the broker ack so callers can batch many sends and await them together.
     * The future fails with {@link KafkaPublishException} if the send does not complete successfully.
     */
    public CompletableFuture<Void> publishAsync(StoreRecord storeRecord) {
        log.debug("Sending storeId={} version={} to topic={}", storeRecord.getLocationId(), storeRecord.getVersion(), topic);
        String failure = "Failed to publish storeId=" + storeRecord.getLocationId() + " to topic " + topic;
        try {
            return kafkaTemplate.send(topic, storeRecord.getLocationId(), storeRecord)
                    .orTimeout(KafkaPublishUtility.PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .handle((result, ex) -> {
                        if (ex != null) {
                            log.error("{}", failure, ex);
                            throw new KafkaPublishException(failure, ex);
                        }
                        return (Void) null;
                    });
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(new KafkaPublishException(failure, e));
        }
    }

    /**
     * Publishes the store's payload JSON to the store-events topic, keyed by locationId.
     */
    public void publish(StoreRecord storeRecord) {
        SendResult<String, StoreRecord> result = KafkaPublishUtility.awaitSend(
                kafkaTemplate.send(topic, storeRecord.getLocationId(), storeRecord),
                "publish storeId=" + storeRecord.getLocationId() + " to topic " + topic);

        log.debug("Published store event storeId={} to topic={} partition={} offset={}",
                storeRecord.getLocationId(), topic,
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
    }
}
