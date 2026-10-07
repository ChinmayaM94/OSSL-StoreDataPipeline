package com.safeway.ossl.storepipeline.service;

import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.exception.KafkaPublishException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes a versioned {@link StoreRecord} to Kafka Topic 1
 * ({@code ossl-store-publication}), keyed by storeId, as part of
 * {@code POST /api/stores/{storeId}/publish}.ß
 */
@Service
public class StorePublicationProducerService {

    private static final Logger log = LoggerFactory.getLogger(StorePublicationProducerService.class);
    private static final long PUBLISH_TIMEOUT_SECONDS = 5;

    private final KafkaTemplate<String, StoreRecord> kafkaTemplate;
    private final String topic;

    public StorePublicationProducerService(KafkaTemplate<String, StoreRecord> storeRecordKafkaTemplate,
                                           @Value("${kafka.topic.store-publication}") String topic) {
        this.kafkaTemplate = storeRecordKafkaTemplate;
        this.topic = topic;
    }

    /**
     * Publishes the given versioned store record to the store-publication topic.
     *
     * @throws KafkaPublishException if the send fails, times out, or is interrupted.
     */
    public void publish(StoreRecord storeRecord) {
        try {
            SendResult<String, StoreRecord> result = kafkaTemplate
                    .send(topic, storeRecord.getLocationId(), storeRecord)
                    .get(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            log.debug("Published store publication storeId={} version={} to topic={} partition={} offset={}",
                    storeRecord.getLocationId(), storeRecord.getVersion(), topic,
                    result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException(
                    "Interrupted while publishing storeId=" + storeRecord.getLocationId() + " to topic " + topic, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new KafkaPublishException(
                    "Failed to publish storeId=" + storeRecord.getLocationId() + " to topic " + topic, e);
        }
    }
}
