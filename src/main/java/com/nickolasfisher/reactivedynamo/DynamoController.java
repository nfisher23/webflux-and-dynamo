package com.nickolasfisher.reactivedynamo;


import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

@RestController
public class DynamoController {

    public static final String PHONES_TABLENAME = "Phones";
    public static final String COMPANY = "Company";
    public static final String MODEL = "Model";
    private final DynamoDbAsyncClient dynamoDbAsyncClient;

    public DynamoController(DynamoDbAsyncClient dynamoDbAsyncClient) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    }

    @PutMapping("/phone")
    public Mono<Void> upsert(@RequestBody Phone phone) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(COMPANY, AttributeValue.builder().s(phone.getCompany()).build());
        item.put(MODEL, AttributeValue.builder().s(phone.getModel()).build());
        item.put("Colors", AttributeValue.builder().ss(phone.getColors()).build());
        if (phone.getSize() != null) {
            item.put("Size", AttributeValue.builder().n(phone.getSize().toString()).build());
        }

        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(PHONES_TABLENAME).item(item).build();

        // TODO: error handling
        return Mono.fromCompletionStage(dynamoDbAsyncClient.putItem(putItemRequest)).then();
    }

    @GetMapping("/company/{company-name}/model/{model-name}/phone")
    @ExceptionHandler
    public Mono<Phone> getPhone(@PathVariable("company-name") String companyName,
                                @PathVariable("model-name") String modelName) {
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
                        return Mono.error(new NotFoundException());
                    }
                    Phone phone = new Phone();
                    phone.setColors(getItemResponse.item().get("Colors").ss());
                    return Mono.just(phone);
                });
    }

    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Mono<Phone> handleException(@PathVariable("company-name") String companyName,
                                       @PathVariable("model-name") String modelName) {
        return Mono.empty();
    }

    public class NotFoundException extends RuntimeException {}
}
