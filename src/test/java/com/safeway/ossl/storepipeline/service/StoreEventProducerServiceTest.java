package com.safeway.ossl.storepipeline.service;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.exception.KafkaPublishException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StoreEventProducerServiceTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, StoreRecord> kafkaTemplate = Mockito.mock(KafkaTemplate.class);
    private final StoreEventProducerService service = new StoreEventProducerService(kafkaTemplate, "store-events");

    @Test
    void publish_sendsRecordToKafkaTopic() {
        StoreRecord record = record("1001");
        @SuppressWarnings("unchecked")
        SendResult<String, StoreRecord> sendResult = Mockito.mock(SendResult.class);
        RecordMetadata metadata = Mockito.mock(RecordMetadata.class);
        when(sendResult.getRecordMetadata()).thenReturn(metadata);
        when(metadata.partition()).thenReturn(1);
        when(metadata.offset()).thenReturn(12L);
        when(kafkaTemplate.send("store-events", "1001", record))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        service.publish(record);

        verify(kafkaTemplate).send("store-events", "1001", record);
    }

    @Test
    void publish_wrapsInterruptedExceptionAndReinterruptsThread() {
        StoreRecord record = record("1001");
        when(kafkaTemplate.send(eq("store-events"), eq("1001"), eq(record)))
                .thenReturn(new InterruptedFuture<>());

        KafkaPublishException exception = assertThrows(KafkaPublishException.class, () -> service.publish(record));

        assertInstanceOf(InterruptedException.class, exception.getCause());
        assertTrue(Thread.currentThread().isInterrupted());
        Thread.interrupted();
    }

    @Test
    void publish_wrapsTimeoutException() {
        StoreRecord record = record("1001");
        when(kafkaTemplate.send(eq("store-events"), eq("1001"), eq(record)))
                .thenReturn(new TimeoutFuture<>());

        KafkaPublishException exception = assertThrows(KafkaPublishException.class, () -> service.publish(record));

        assertInstanceOf(TimeoutException.class, exception.getCause());
    }

    private static StoreRecord record(String locationId) {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("phone", "555-1111");
        return new StoreRecord(locationId, payload, 2L);
    }

    private static final class InterruptedFuture<T> extends CompletableFuture<T> {
        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            throw new InterruptedException("interrupted");
        }
    }

    private static final class TimeoutFuture<T> extends CompletableFuture<T> {
        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            throw new TimeoutException("timeout");
        }
    }
}

