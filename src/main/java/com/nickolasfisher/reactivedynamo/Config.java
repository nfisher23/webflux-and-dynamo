package com.nickolasfisher.reactivedynamo;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.*;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import java.net.URI;

import static org.springframework.web.reactive.function.server.RouterFunctions.route;

@Configuration
public class Config {

    @Bean
    public DynamoDbAsyncClient dynamoDbAsyncClient() {
        return DynamoDbAsyncClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("FAKE", "FAKE")))
                .region(Region.US_WEST_2)
                .endpointOverride(URI.create("http://localhost:8000"))
                .build();
    }

    @Bean
    public RouterFunction<ServerResponse> getPhoneRoutes(PhoneHandler phoneHandler) {
        return route(RequestPredicates.PUT("/phone"), phoneHandler::createPhoneHandler)
                .andRoute(RequestPredicates.GET("/company/{company-name}/model/{model-name}/phone"), phoneHandler::getSinglePhonxeHandler);
    }
}
