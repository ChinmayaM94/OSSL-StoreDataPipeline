package com.safeway.ossl.storepipeline.service;

import com.safeway.ossl.storepipeline.exception.KafkaPublishException;
import com.safeway.ossl.storepipeline.utility.KafkaPublishUtility;
import lombok.Getter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Replays dead-lettered store events: reads everything currently in
 * {@code <store-events topic>.DLQ} and publishes it back to the store-events topic
 * with the original key, so both the SQL and Mongo consumers get another try.
 * <p>
 * Offsets in the DLQ are committed only after a message has been republished, so a
 * failed replay can simply be run again. Replaying is safe because both consumers are
 * idempotent (SQL upsert never lowers the version; Mongo applies only newer versions).
 * A message that still fails after the replay goes through retry and lands in the DLQ again.
 */
@Service
public class DlqReplayService {

    private static final Logger log = LoggerFactory.getLogger(DlqReplayService.class);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(3);
    private static final int MAX_POLLS_WAITING_FOR_ASSIGNMENT = 10;

    private final ConsumerFactory<String, String> consumerFactory;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String sourceTopic;
    @Getter
    private final String dlqTopic;

    public DlqReplayService(@Qualifier("dlqReplayConsumerFactory") ConsumerFactory<String, String> consumerFactory,
                            @Qualifier("dlqReplayKafkaTemplate") KafkaTemplate<String, String> kafkaTemplate,
                            @Value("${kafka.topic.store-events}") String sourceTopic) {
        this.consumerFactory = consumerFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.sourceTopic = sourceTopic;
        this.dlqTopic = sourceTopic + ".DLQ";
    }

    /**
     * Republishes all messages currently in the DLQ to the store-events topic.
     *
     * @return number of messages replayed
     * @throws KafkaPublishException if a republish fails; messages replayed before it stay committed
     */
    public synchronized int replay() {
        int replayed = 0;
        int waitingPolls = 0;
        log.info("Starting DLQ replay from {} to {}", dlqTopic, sourceTopic);

        try (Consumer<String, String> consumer = consumerFactory.createConsumer()) {
            consumer.subscribe(List.of(dlqTopic));

            boolean wasAssigned = false;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);

                if (records.isEmpty()) {
                    // The poll that completes the group join can return empty even though records exist,
                    // so the DLQ only counts as drained after an empty poll made with an assignment already in place.
                    if (wasAssigned || ++waitingPolls >= MAX_POLLS_WAITING_FOR_ASSIGNMENT) {
                        break;
                    }
                    wasAssigned = !consumer.assignment().isEmpty();
                    continue;
                }
                wasAssigned = true;
                log.debug("DLQ replay polled {} records", records.count());

                Map<TopicPartition, OffsetAndMetadata> done = new HashMap<>();
                try {
                    for (ConsumerRecord<String, String> record : records) {
                        republish(record);
                        done.put(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1));
                        replayed++;
                    }
                } finally {
                    if (!done.isEmpty()) {
                        consumer.commitSync(done);
                    }
                }
            }
        }

        log.info("DLQ replay finished: replayed={} from {} to {}", replayed, dlqTopic, sourceTopic);
        return replayed;
    }

    private void republish(ConsumerRecord<String, String> record) {
        log.debug("Replaying key={} partition={} offset={} to {}", record.key(), record.partition(), record.offset(), sourceTopic);
        KafkaPublishUtility.awaitSend(
                kafkaTemplate.send(new ProducerRecord<>(sourceTopic, record.key(), record.value())),
                "replay key=" + record.key() + " to " + sourceTopic);
    }
}
