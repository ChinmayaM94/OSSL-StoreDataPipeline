package com.safeway.ossl.storepipeline.config;

import com.safeway.ossl.storepipeline.dto.StoreRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlerConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            @Qualifier("storeRecordKafkaTemplate") KafkaTemplate<String, StoreRecord> kafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) ->
                        new TopicPartition(record.topic() + ".DLQ", record.partition()));

        FixedBackOff backOff = new FixedBackOff(1000L, 1L);

        return new DefaultErrorHandler(recoverer, backOff);
    }
}