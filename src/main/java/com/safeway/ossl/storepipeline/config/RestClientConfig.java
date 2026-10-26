package com.safeway.ossl.storepipeline.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;

/**
 * HTTP client for the ossl-store-consumer-service microservice. Inject it (by the
 * {@code consumerServiceRestClient} qualifier) into any service that needs to call that service.
 */
@Configuration
public class RestClientConfig {

    @Bean
    public RestClient consumerServiceRestClient(
            RestClient.Builder builder,
            @Value("${consumer-service.base-url}") String baseUrl,
            @Value("${consumer-service.connect-timeout}") Duration connectTimeout,
            @Value("${consumer-service.read-timeout}") Duration readTimeout) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(connectTimeout);
        requestFactory.setReadTimeout(readTimeout);
        return builder
                .baseUrl(baseUrl)
                .requestFactory(requestFactory)
                .build();
    }
}
