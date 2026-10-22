package com.safeway.ossl.storepipeline.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.safeway.ossl.storepipeline.dto.StoreChangeEvent;
import com.safeway.ossl.storepipeline.utility.KafkaPublishUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class ChangeEventProducerService {

    private static final Logger log = LoggerFactory.getLogger(ChangeEventProducerService.class);

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
        JsonNode payload = objectMapper.valueToTree(event);

        SendResult<String, JsonNode> result = KafkaPublishUtility.awaitSend(
                kafkaTemplate.send(topic, event.getLocationId(), payload),
                "publish change event for locationId=" + event.getLocationId() + " to topic " + topic);

        log.debug("Published change event locationId={} version={} to topic={} partition={} offset={}",
                event.getLocationId(), event.getVersion(), topic,
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
    }
}
