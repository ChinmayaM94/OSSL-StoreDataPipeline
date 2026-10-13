package com.safeway.ossl.storepipeline.config;

import com.safeway.ossl.storepipeline.dto.StoreChangeEvent;
import com.safeway.ossl.storepipeline.dto.StoreRecord;
import com.safeway.ossl.storepipeline.listener.StoreEventConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for the two downstream listeners:
 * {@link StoreEventConsumer}
 * (Topic 1: {@code ossl-store-publication}) and
 * {@link com.safeway.ossl.storepipeline.listener.ChangeEventConsumer}
 * (Topic 2: {@code ossl-store-change-events}).
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group-id}")
    private String groupId;

    private Map<String, Object> baseConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.safeway.ossl.storepipeline.dto");
        return props;
    }

    @Bean
    public ConsumerFactory<String, StoreRecord> storeRecordConsumerFactory() {
        Map<String, Object> props = baseConsumerProps();
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new ErrorHandlingDeserializer<>(new JsonDeserializer<>(StoreRecord.class, false)));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, StoreRecord> storeRecordKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, StoreRecord> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(storeRecordConsumerFactory());
        factory.setCommonErrorHandler(defaultErrorHandler());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, StoreChangeEvent> storeChangeEventConsumerFactory() {
        Map<String, Object> props = baseConsumerProps();
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new ErrorHandlingDeserializer<>(new JsonDeserializer<>(StoreChangeEvent.class, false)));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, StoreChangeEvent> storeChangeEventKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, StoreChangeEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(storeChangeEventConsumerFactory());
        factory.setCommonErrorHandler(defaultErrorHandler());
        return factory;
    }

    private DefaultErrorHandler defaultErrorHandler() {
        // Retry twice with a 1s backoff, then log-and-skip rather than redeliver forever.
        DefaultErrorHandler handler = new DefaultErrorHandler(
                (record, exception) -> log.error(
                        "Skipping unprocessable Kafka message after retries exhausted. topic={} partition={} offset={}",
                        record.topic(), record.partition(), record.offset(), exception),
                new FixedBackOff(1000L, 2L));
        return handler;
    }
}
