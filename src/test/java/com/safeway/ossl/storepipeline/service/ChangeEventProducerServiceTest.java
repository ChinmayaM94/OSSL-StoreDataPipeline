package com.safeway.ossl.storepipeline.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.safeway.ossl.storepipeline.dto.StoreChangeEvent;
import com.safeway.ossl.storepipeline.exception.KafkaPublishException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChangeEventProducerServiceTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, JsonNode> kafkaTemplate = Mockito.mock(KafkaTemplate.class);
    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
    private final ChangeEventProducerService service =
            new ChangeEventProducerService(kafkaTemplate, "change-events", objectMapper);

    @Test
    void publish_serializesEventAndSendsToKafka() {
        StoreChangeEvent event = new StoreChangeEvent("1001", 3L, Instant.parse("2026-10-13T10:15:30Z"));
        @SuppressWarnings("unchecked")
        SendResult<String, JsonNode> sendResult = Mockito.mock(SendResult.class);
        RecordMetadata metadata = Mockito.mock(RecordMetadata.class);
        when(sendResult.getRecordMetadata()).thenReturn(metadata);
        when(metadata.partition()).thenReturn(1);
        when(metadata.offset()).thenReturn(8L);
        when(kafkaTemplate.send(eq("change-events"), eq("1001"), any(JsonNode.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        service.publish(event);

        ArgumentCaptor<JsonNode> payloadCaptor = ArgumentCaptor.forClass(JsonNode.class);
        verify(kafkaTemplate).send(eq("change-events"), eq("1001"), payloadCaptor.capture());
        JsonNode payload = payloadCaptor.getValue();
        assertEquals("1001", payload.path("locationId").asText());
        assertEquals(3L, payload.path("version").asLong());
        assertEquals(Instant.parse("2026-10-13T10:15:30Z").getEpochSecond(), payload.path("changeTimestamp").asLong());
    }

    @Test
    void publish_wrapsExecutionFailures() {
        StoreChangeEvent event = new StoreChangeEvent("1001", 3L, Instant.now());
        CompletableFuture<SendResult<String, JsonNode>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("broker unavailable"));
        when(kafkaTemplate.send(eq("change-events"), eq("1001"), any(JsonNode.class))).thenReturn(failed);

        KafkaPublishException exception = assertThrows(KafkaPublishException.class, () -> service.publish(event));

        assertInstanceOf(ExecutionException.class, exception.getCause());
    }

    @Test
    void publish_wrapsTimeoutException() {
        StoreChangeEvent event = new StoreChangeEvent("1001", 3L, Instant.now());
        when(kafkaTemplate.send(eq("change-events"), eq("1001"), any(JsonNode.class)))
                .thenReturn(new TimeoutFuture<>());

        KafkaPublishException exception = assertThrows(KafkaPublishException.class, () -> service.publish(event));

        assertInstanceOf(TimeoutException.class, exception.getCause());
    }

    private static final class TimeoutFuture<T> extends CompletableFuture<T> {
        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            throw new TimeoutException("timeout");
        }
    }
}

