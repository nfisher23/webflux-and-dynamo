package com.nickolasfisher.reactivedynamo;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

public class PhoneServiceTest {

    public static final String COMPANY = "Company";
    public static final String MODEL = "Model";
    private static DynamoDBProxyServer dynamoProxy;

    private static DynamoDbAsyncClient dynamoDbAsyncClient;

    private static int port;
    public static final String COLOR = "Color";
    public static final String YEAR = "Year";

    private static int getFreePort() {
        try {
            ServerSocket socket = new ServerSocket(0);
            int port = socket.getLocalPort();
            socket.close();
            return port;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @BeforeAll
    public static void setupDynamo() {
        port = getFreePort();
        try {
            dynamoProxy = ServerRunner.createServerFromCommandLineArgs(new String[]{
                    "-inMemory",
                    "-port",
                    Integer.toString(port)
            });
            dynamoProxy.start();

            dynamoDbAsyncClient = getDynamoClient();
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    @AfterAll
    public static void teardownDynamo() {
        try {
            dynamoProxy.stop();
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    @Test
    public void testStuff() throws Exception {
        ListTablesResponse listTablesResponse = dynamoDbAsyncClient.listTables().get();

        int totalTablesBeforeCreation = listTablesResponse.tableNames().size();

        createTableAsync("Phones").get();

        ListTablesResponse listTablesResponseAfterCreation = dynamoDbAsyncClient.listTables().get();

        assertThat(listTablesResponseAfterCreation.tableNames().size()).isEqualTo(totalTablesBeforeCreation + 1);
    }

    private static DynamoDbAsyncClient getDynamoClient() {
        return DynamoDbAsyncClient.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create("http://localhost:" + port))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("FAKE", "FAKE")))
                .build();
    }

    private CompletableFuture<CreateTableResponse> createTableAsync(String tableName) {
        return dynamoDbAsyncClient.createTable(CreateTableRequest.builder()
                .keySchema(
                        KeySchemaElement.builder()
                                .keyType(KeyType.HASH)
                                .attributeName(COMPANY)
                                .build(),
                        KeySchemaElement.builder()
                                .keyType(KeyType.RANGE)
                                .attributeName(MODEL)
                                .build()
                )
                .attributeDefinitions(
                        AttributeDefinition.builder()
                                .attributeName(COMPANY)
                                .attributeType(ScalarAttributeType.S)
                                .build(),
                        AttributeDefinition.builder()
                                .attributeName(MODEL)
                                .attributeType(ScalarAttributeType.S)
                                .build()
                )
                .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(100L).writeCapacityUnits(100L).build())
                .tableName(tableName)
                .build()
        );
    }

    @Test
    public void testOptimisticLocking() throws Exception {
        String currentTableName = "PhonesOptLocking";

        createTableAndWaitForComplete(currentTableName);

        String stubCompanyName = "Nokia";
        String stubPhoneName = "flip-phone-1";

        Map<String, AttributeValue> itemAttributes = getMapWith(stubCompanyName, stubPhoneName);
        itemAttributes.put("Color", AttributeValue.builder().s("Orange").build());
        itemAttributes.put("Version", AttributeValue.builder().n(Long.valueOf(1L).toString()).build());

        PutItemRequest populateDataItemRequest = PutItemRequest.builder()
                .tableName(currentTableName)
                .item(itemAttributes)
                .build();

        // populate initial data
        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.putItem(populateDataItemRequest)))
                .expectNextCount(1)
                .verifyComplete();

        Map<String, AttributeValue> itemAttributesOptLocking = getMapWith(stubCompanyName, stubPhoneName);

        itemAttributesOptLocking.put("Color", AttributeValue.builder().s("Blue").build());
        itemAttributesOptLocking.put("Version", AttributeValue.builder().n(Long.valueOf(1L).toString()).build());

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":version", AttributeValue.builder().n("0").build());

        PutItemRequest conditionalPutItem = PutItemRequest.builder()
                .item(itemAttributes)
                .tableName(currentTableName)
                .conditionExpression("Version = :version")
                .expressionAttributeValues(expressionAttributeValues)
                .build();

        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.putItem(conditionalPutItem)))
                .expectErrorMatches(throwable -> throwable instanceof ConditionalCheckFailedException)
                .verify();

        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.getItem(
                GetItemRequest.builder()
                        .tableName(currentTableName)
                        .key(getMapWith(stubCompanyName, stubPhoneName))
                        .build())
        ))
                // not blue, so our conditional expression prevented us from overwriting it
                .expectNextMatches(getItemResponse -> "Orange".equals(getItemResponse.item().get("Color").s()))
                .verifyComplete();
    }

    @Test
    public void testQueries() throws Exception {
        String currentTableName = "PhonesQueriesTest";
        createTableAndWaitForComplete(currentTableName);

        String partitionKey = "Google";
        String rangeKey1 = "Pixel 1";
        String rangeKey2 = "Future Phone";
        String rangeKey3 = "Pixel 2";

        // create three items
        Map<String, AttributeValue> pixel1ItemAttributes = getMapWith(partitionKey, rangeKey1);
        pixel1ItemAttributes.put(COLOR, AttributeValue.builder().s("Blue").build());
        pixel1ItemAttributes.put(YEAR, AttributeValue.builder().n("2012").build());
        putItem(currentTableName, pixel1ItemAttributes);

        Map<String, AttributeValue> futurePhoneAttributes = getMapWith(partitionKey, rangeKey2);
        futurePhoneAttributes.put(COLOR, AttributeValue.builder().s("Silver").build());
        futurePhoneAttributes.put(YEAR, AttributeValue.builder().n("2030").build());
        putItem(currentTableName, futurePhoneAttributes);

        Map<String, AttributeValue> pixel2ItemAttributes = getMapWith(partitionKey, rangeKey3);
        pixel2ItemAttributes.put(COLOR, AttributeValue.builder().s("Cyan").build());
        pixel2ItemAttributes.put(YEAR, AttributeValue.builder().n("2014").build());
        putItem(currentTableName, pixel2ItemAttributes);

        // get all items associated with the "Google" partition key
        Condition equalsGoogleCondition = Condition.builder()
                .comparisonOperator(ComparisonOperator.EQ)
                .attributeValueList(
                        AttributeValue.builder()
                                .s(partitionKey)
                                .build()
                )
                .build();

        QueryRequest hashKeyIsGoogleQuery = QueryRequest.builder()
                .tableName(currentTableName)
                .keyConditions(Map.of(COMPANY, equalsGoogleCondition))
                .build();

        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.query(hashKeyIsGoogleQuery)))
                .expectNextMatches(queryResponse -> queryResponse.count() == 3
                        && queryResponse.items()
                        .stream()
                        .anyMatch(attributeValueMap -> "2012".equals(
                                attributeValueMap.get(YEAR).n())
                        )
                )
                .verifyComplete();

        // Get all items that start with "Pixel"
        Condition startsWithPixelCondition = Condition.builder()
                .comparisonOperator(ComparisonOperator.BEGINS_WITH)
                .attributeValueList(
                        AttributeValue.builder()
                                .s("Pixel")
                                .build()
                )
                .build();

        QueryRequest equalsGoogleAndStartsWithPixelQuery = QueryRequest.builder()
                .tableName(currentTableName)
                .keyConditions(Map.of(
                        COMPANY, equalsGoogleCondition,
                        MODEL, startsWithPixelCondition
                ))
                .build();

        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.query(equalsGoogleAndStartsWithPixelQuery)))
                .expectNextMatches(queryResponse -> queryResponse.count() == 2)
                .verifyComplete();
    }

    @Test
    public void testTTL() throws Exception {
        String currentTableName = "PhoneTTLTest";
        createTableAndWaitForComplete(currentTableName);

        String EXPIRE_TIME = "ExpireTime";
        dynamoDbAsyncClient.updateTimeToLive(
            UpdateTimeToLiveRequest.builder()
                .tableName(currentTableName)
                .timeToLiveSpecification(
                        TimeToLiveSpecification.builder()
                                .enabled(true)
                                .attributeName(EXPIRE_TIME)
                                .build()
                )
                .build()
        ).get();

        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.describeTimeToLive(
                DescribeTimeToLiveRequest.builder().tableName(currentTableName).build()))
            )
            .expectNextMatches(describeTimeToLiveResponse ->
                describeTimeToLiveResponse
                    .timeToLiveDescription()
                    .timeToLiveStatus().equals(TimeToLiveStatus.ENABLED)
            )
            .verifyComplete();

        String partitionKey = "Google";
        String rangeKey = "Pixel 1";

        Map<String, AttributeValue> pixel1ItemAttributes = getMapWith(partitionKey, rangeKey);
        pixel1ItemAttributes.put(COLOR, AttributeValue.builder().s("Blue").build());
        pixel1ItemAttributes.put(YEAR, AttributeValue.builder().n("2012").build());

        // expire about 3 seconds from now
        String expireTime = Long.toString((System.currentTimeMillis() / 1000L) + 3);
        pixel1ItemAttributes.put(
                EXPIRE_TIME,
                AttributeValue.builder()
                        .n(expireTime)
                        .build()
        );

        PutItemRequest populateDataItemRequest = PutItemRequest.builder()
                .tableName(currentTableName)
                .item(pixel1ItemAttributes)
                .build();

        // put item with TTL into dynamo
        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.putItem(populateDataItemRequest)))
                .expectNextCount(1)
                .verifyComplete();

        Map<String, AttributeValue> currentItemKey = Map.of(
                COMPANY, AttributeValue.builder().s(partitionKey).build(),
                MODEL, AttributeValue.builder().s(rangeKey).build()
        );

        // get immediately, should exist
        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.getItem(
                GetItemRequest.builder().tableName(currentTableName).key(currentItemKey).build()))
            )
            .expectNextMatches(getItemResponse -> getItemResponse.hasItem()
                    && getItemResponse.item().get(COLOR).s().equals("Blue"))
            .verifyComplete();

        // local dynamo seems to need like 10 seconds to actually clear this out
        Thread.sleep(13000);

        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.getItem(
                GetItemRequest.builder()
                        .key(currentItemKey)
                        .tableName(currentTableName)
                        .build())
                )
            )
            .expectNextMatches(getItemResponse -> !getItemResponse.hasItem())
            .verifyComplete();
    }

    private void putItem(String tableName, Map<String, AttributeValue> attributes) {
        PutItemRequest populateDataItemRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(attributes)
                .build();

        // populate initial data
        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.putItem(populateDataItemRequest)))
                .expectNextCount(1)
                .verifyComplete();
    }

    private void createTableAndWaitForComplete(String currentTableName) throws InterruptedException, java.util.concurrent.ExecutionException {
        createTableAsync(currentTableName).get();

        Mono.fromFuture(() -> dynamoDbAsyncClient.describeTable(DescribeTableRequest.builder().tableName(currentTableName).build()))
                .flatMap(describeTableResponse -> {
                    if (describeTableResponse.table().tableStatus() == TableStatus.ACTIVE) {
                        return Mono.just(describeTableResponse);
                    } else {
                        return Mono.error(new RuntimeException());
                    }
                })
                .retry(100).block();
    }

    private Map<String, AttributeValue> getMapWith(String companyName, String modelName) {
        Map<String, AttributeValue> map = new HashMap<>();

        map.put(COMPANY, AttributeValue.builder().s(companyName).build());
        map.put(MODEL, AttributeValue.builder().s(modelName).build());

        return map;
    }
}
