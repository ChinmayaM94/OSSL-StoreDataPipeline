package com.safeway.ossl.storepipeline.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Basic OpenAPI/Swagger metadata. UI is available at /swagger-ui.html and
 * the raw spec at /v3/api-docs once the application is running.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI osslStorePipelineOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("OSSL Store Data Pipeline API")
                        .description("REST APIs for OSSL-StoreData micro-service")
                        .version("1.0.0")
                        .contact(new Contact().name("OSSL Store Pipeline Team")));
    }
}
