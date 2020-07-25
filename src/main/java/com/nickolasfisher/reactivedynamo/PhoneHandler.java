package com.nickolasfisher.reactivedynamo;

import org.reactivestreams.Publisher;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@Component
public class PhoneHandler {

    public static final String PHONES_TABLENAME = "Phones";
    public static final String COMPANY = "Company";
    public static final String MODEL = "Model";

    private final DynamoDbAsyncClient dynamoDbAsyncClient;

    public PhoneHandler(DynamoDbAsyncClient dynamoDbAsyncClient) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    }

    public Mono<ServerResponse> createPhoneHandler(ServerRequest serverRequest) {
        return serverRequest.bodyToMono(Phone.class).flatMap(phone -> {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put(COMPANY, AttributeValue.builder().s(phone.getCompany()).build());
            item.put(MODEL, AttributeValue.builder().s(phone.getModel()).build());
            item.put("Colors", AttributeValue.builder().ss(phone.getColors()).build());
            if (phone.getSize() != null) {
                item.put("Size", AttributeValue.builder().n(phone.getSize().toString()).build());
            }

            PutItemRequest putItemRequest = PutItemRequest.builder().tableName(PHONES_TABLENAME).item(item).build();

            return Mono.fromCompletionStage(dynamoDbAsyncClient.putItem(putItemRequest))
                    .flatMap(putItemResponse -> ServerResponse.ok().build());
        });
    }

    public Mono<ServerResponse> getSinglePhoneHandler(ServerRequest serverRequest) {
        String companyName = serverRequest.pathVariable("company-name");
        String modelName = serverRequest.pathVariable("model-name");

        Map<String, AttributeValue> getSinglePhoneItemRequest = new HashMap<>();
        getSinglePhoneItemRequest.put(COMPANY, AttributeValue.builder().s(companyName).build());
        getSinglePhoneItemRequest.put(MODEL, AttributeValue.builder().s(modelName).build());
        GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName(PHONES_TABLENAME)
                .key(getSinglePhoneItemRequest)
                .build();

        CompletableFuture<GetItemResponse> item = dynamoDbAsyncClient.getItem(getItemRequest);
        return Mono.fromCompletionStage(item)
                .flatMap(getItemResponse -> {
                    if (!getItemResponse.hasItem()) {
                        return ServerResponse.notFound().build();
                    }
                    Phone phone = new Phone();
                    phone.setColors(getItemResponse.item().get("Colors").ss());
                    return ServerResponse.ok()
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(BodyInserters.fromValue(phone));
                });
    }
}
