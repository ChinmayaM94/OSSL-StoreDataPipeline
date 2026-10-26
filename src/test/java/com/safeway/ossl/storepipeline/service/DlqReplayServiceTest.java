package com.safeway.ossl.storepipeline.service;

import com.safeway.ossl.storepipeline.dto.DlqReplayResponse;
import com.safeway.ossl.storepipeline.exception.ConsumerServiceException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class DlqReplayServiceTest {

    private static final String URL = "http://consumer:8081/api/stores/dlq/replay";

    private MockRestServiceServer server;
    private DlqReplayService service;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder().baseUrl("http://consumer:8081");
        server = MockRestServiceServer.bindTo(builder).build();
        service = new DlqReplayService(builder.build());
    }

    @Test
    void replay_postsToConsumerServiceAndMapsResponse() {
        server.expect(requestTo(URL)).andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess("{\"dlqTopic\":\"ossl-store-events.DLQ\",\"replayed\":3}", MediaType.APPLICATION_JSON));

        DlqReplayResponse response = service.replay();

        assertEquals("ossl-store-events.DLQ", response.dlqTopic());
        assertEquals(3, response.replayed());
        server.verify();
    }

    @Test
    void replay_consumerReturnsError_mapsTo502() {
        server.expect(requestTo(URL)).andRespond(withServerError());

        ConsumerServiceException ex = assertThrows(ConsumerServiceException.class, service::replay);

        assertEquals(HttpStatus.BAD_GATEWAY, ex.getStatus());
    }

    @Test
    void replay_consumerUnreachable_mapsTo503() {
        server.expect(requestTo(URL)).andRespond(request -> { throw new IOException("connection refused"); });

        ConsumerServiceException ex = assertThrows(ConsumerServiceException.class, service::replay);

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, ex.getStatus());
    }
}
