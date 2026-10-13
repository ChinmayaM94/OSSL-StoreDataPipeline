package com.safeway.ossl.storepipeline.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.safeway.ossl.storepipeline.dto.StoreChangeEvent;
import com.safeway.ossl.storepipeline.exception.KafkaPublishException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class ChangeEventProducerService {

    private static final Logger log = LoggerFactory.getLogger(ChangeEventProducerService.class);
    private static final long PUBLISH_TIMEOUT_SECONDS = 5;

    private final KafkaTemplate<String, JsonNode> kafkaTemplate;
    private final String topic;
    private final ObjectMapper objectMapper;

    public ChangeEventProducerService(
            @Qualifier("storePayloadKafkaTemplate") KafkaTemplate<String, JsonNode> changeEventKafkaTemplate,
            @Value("${kafka.topic.change-events}") String topic,
            ObjectMapper objectMapper) {
        this.kafkaTemplate = changeEventKafkaTemplate;
        this.topic = topic;
        this.objectMapper = objectMapper;
    }

    public void publish(StoreChangeEvent event) {
        try {
            JsonNode payload = objectMapper.valueToTree(event);

            SendResult<String, JsonNode> result = kafkaTemplate
                    .send(topic, event.getLocationId(), payload)
                    .get(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            log.debug("Published change event locationId={} version={} to topic={} partition={} offset={}",
                    event.getLocationId(), event.getVersion(), topic,
                    result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException(
                    "Interrupted while publishing change event for locationId=" + event.getLocationId()
                            + " to topic " + topic, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new KafkaPublishException(
                    "Failed to publish change event for locationId=" + event.getLocationId()
                            + " to topic " + topic, e);
        }
    }
}