package com.safeway.ossl.storepipeline.service;

import com.safeway.ossl.storepipeline.dto.DlqReplayResponse;
import com.safeway.ossl.storepipeline.exception.ConsumerServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import static com.safeway.ossl.storepipeline.constants.OSSLServiceEndPoints.DLQ_REPLAY_URL;
import static com.safeway.ossl.storepipeline.constants.OSSLServiceEndPoints.OSSL_SERVICE_API_PREFIX;

/**
 * Replays dead-lettered store events. The replay itself runs in the ossl-store-consumer-service
 * microservice (it owns the consumers and the DLQ); this service calls its REST API.
 */
@Service
public class DlqReplayService {

    private static final Logger log = LoggerFactory.getLogger(DlqReplayService.class);

    private final RestClient restClient;

    public DlqReplayService(@Qualifier("consumerServiceRestClient") RestClient restClient) {
        this.restClient = restClient;
    }

    /** Asks the consumer service to replay its dead-letter topic back to the store-events topic. */
    public DlqReplayResponse replay() {
        try {
            return restClient.post()
                    .uri(OSSL_SERVICE_API_PREFIX + DLQ_REPLAY_URL)
                    .retrieve()
                    .body(DlqReplayResponse.class);
        } catch (ResourceAccessException ex) {
            log.error("Consumer service unreachable: {}", ex.getMessage());
            throw new ConsumerServiceException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Consumer service is unreachable. Make sure ossl-store-consumer-service is running.", ex);
        } catch (RestClientResponseException ex) {
            log.error("Consumer service returned {}: {}", ex.getStatusCode(), ex.getResponseBodyAsString());
            throw new ConsumerServiceException(HttpStatus.BAD_GATEWAY,
                    "Consumer service failed to replay the DLQ (HTTP " + ex.getStatusCode().value() + ").", ex);
        }
    }
}
