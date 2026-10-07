package com.safeway.ossl.storepipeline.service;

import com.fasterxml.jackson.databind.JsonNode;
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

@Service
public class StoreEventProducerService {

    private static final Logger log = LoggerFactory.getLogger(StoreEventProducerService.class);
    private static final long PUBLISH_TIMEOUT_SECONDS = 5;

    private final KafkaTemplate<String, StoreRecord> kafkaTemplate;
    private final String topic;

    public StoreEventProducerService(KafkaTemplate<String, StoreRecord> storePayloadKafkaTemplate,
                                      @Value("${kafka.topic.store-events}") String topic) {
        this.kafkaTemplate = storePayloadKafkaTemplate;
        this.topic = topic;
    }

    /**
     * Publishes just the store's payload JSON (no locationId/payload/version
     * envelope) to the store-events topic, keyed by locationId.
     */
    public void publish(StoreRecord storeRecord) {
        try {
            SendResult<String, StoreRecord> result = kafkaTemplate
                    .send(topic, storeRecord.getLocationId(), storeRecord)
                    .get(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            log.debug("Published store event storeId={} to topic={} partition={} offset={}",
                    storeRecord.getLocationId(), topic,
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