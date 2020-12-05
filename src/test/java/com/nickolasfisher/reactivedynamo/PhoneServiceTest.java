package com.nickolasfisher.reactivedynamo;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.w3c.dom.Attr;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
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

    @Test
    public void localSecondaryIndex() throws Exception {
        String currentTableName = "LocalIndexTest";
        String COMPANY_YEAR_INDEX = "CompanyYearIndex";
        LocalSecondaryIndex localSecondaryIndexSpec = LocalSecondaryIndex.builder()
                .keySchema(
                        KeySchemaElement.builder()
                                .keyType(KeyType.HASH)
                                .attributeName(COMPANY)
                                .build(),
                        KeySchemaElement.builder()
                                .keyType(KeyType.RANGE)
                                .attributeName(YEAR)
                                .build()
                )
                .indexName(COMPANY_YEAR_INDEX)
                .projection(Projection.builder()
                        .projectionType(ProjectionType.ALL)
                        .build()
                )
                .build();

        dynamoDbAsyncClient.createTable(CreateTableRequest.builder()
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
                                .build(),
                        AttributeDefinition.builder()
                                .attributeName(YEAR)
                                .attributeType(ScalarAttributeType.N)
                                .build()
                )
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(100L)
                        .writeCapacityUnits(100L).build()
                )
                .tableName(currentTableName)
                .localSecondaryIndexes(localSecondaryIndexSpec)
                .build()
        ).get();

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

        Condition equalsGoogleCondition = Condition.builder()
                .comparisonOperator(ComparisonOperator.EQ)
                .attributeValueList(
                    AttributeValue.builder()
                            .s(partitionKey)
                            .build()
                )
                .build();

        Condition greaterThan2013Condition = Condition.builder()
                .comparisonOperator(ComparisonOperator.GT)
                .attributeValueList(
                    AttributeValue.builder()
                        .n("2013")
                        .build()
                )
                .build();

        QueryRequest yearAfter2013Query = QueryRequest.builder()
                .tableName(currentTableName)
                .keyConditions(
                    Map.of(
                        COMPANY, equalsGoogleCondition,
                        YEAR, greaterThan2013Condition
                    )
                )
                .indexName(COMPANY_YEAR_INDEX)
                .build();

        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.query(yearAfter2013Query)))
                .expectNextMatches(queryResponse ->
                        queryResponse.count() == 2
                        && queryResponse.items()
                            .stream()
                            .anyMatch(attributeValueMap -> "Pixel 2".equals(
                                    attributeValueMap.get(MODEL).s())
                            )
                )
                .verifyComplete();
    }

    @Test
    public void globalSecondaryIndex() throws Exception {
        String currentTableName = "GlobalSecondaryIndexTest";
        String YEAR_GSI_NAME = "YearModelIndex";

        ProvisionedThroughput defaultProvisionedThroughput = ProvisionedThroughput.builder()
                .readCapacityUnits(100L)
                .writeCapacityUnits(100L)
                .build();

        dynamoDbAsyncClient.createTable(CreateTableRequest.builder()
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
                        .build(),
                    AttributeDefinition.builder()
                        .attributeName(YEAR)
                        .attributeType(ScalarAttributeType.N)
                        .build()
                )
                .provisionedThroughput(defaultProvisionedThroughput)
                .tableName(currentTableName)
                .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                    .indexName(YEAR_GSI_NAME)
                    .keySchema(
                        KeySchemaElement.builder()
                            .attributeName(YEAR)
                            .keyType(KeyType.HASH)
                            .build(),
                        KeySchemaElement.builder()
                            .attributeName(MODEL)
                            .keyType(KeyType.RANGE)
                            .build()
                    ).projection(
                        Projection.builder()
                            .projectionType(ProjectionType.ALL)
                            .build()
                    )
                    .provisionedThroughput(defaultProvisionedThroughput)
                    .build()
            ).build()
        ).get();

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

        Thread.sleep(1000); // GSI's are eventually consistent

        Condition equals2012Condition = Condition.builder()
                .comparisonOperator(ComparisonOperator.EQ)
                .attributeValueList(
                    AttributeValue.builder()
                        .n("2012")
                        .build()
                )
                .build();

        QueryRequest equals2012Query = QueryRequest.builder()
                .tableName(currentTableName)
                .keyConditions(
                    Map.of(
                        YEAR, equals2012Condition
                    )
                )
                .indexName(YEAR_GSI_NAME)
                .build();

        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.query(equals2012Query)))
                .expectNextMatches(queryResponse ->
                    queryResponse.count() == 1
                        && queryResponse.items().get(0).get(COLOR).s().equals("Blue")
                        && queryResponse.items().get(0).get(MODEL).s().equals("Pixel 1")
                )
                .verifyComplete();
    }

    @Test
    public void gsiDuplicateKeysExample() throws Exception {
        String currentTableName = "DuplicateKeysTest";
        String YEAR_GSI_NAME = "YearIndex";

        ProvisionedThroughput defaultProvisionedThroughput = ProvisionedThroughput.builder()
                .readCapacityUnits(100L)
                .writeCapacityUnits(100L)
                .build();

        dynamoDbAsyncClient.createTable(CreateTableRequest.builder()
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
                            .build(),
                    AttributeDefinition.builder()
                            .attributeName(YEAR)
                            .attributeType(ScalarAttributeType.N)
                            .build()
                )
                .provisionedThroughput(defaultProvisionedThroughput)
                .tableName(currentTableName)
                .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                    .indexName(YEAR_GSI_NAME)
                    .keySchema(
                        KeySchemaElement.builder()
                                .attributeName(YEAR)
                                .keyType(KeyType.HASH)
                                .build()
                    ).projection(
                        Projection.builder()
                                .projectionType(ProjectionType.ALL)
                                .build()
                    )
                    .provisionedThroughput(defaultProvisionedThroughput)
                    .build()
                ).build()
        ).get();

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
        futurePhoneAttributes.put(YEAR, AttributeValue.builder().n("2012").build());
        putItem(currentTableName, futurePhoneAttributes);

        Map<String, AttributeValue> pixel2ItemAttributes = getMapWith(partitionKey, rangeKey3);
        pixel2ItemAttributes.put(COLOR, AttributeValue.builder().s("Cyan").build());
        pixel2ItemAttributes.put(YEAR, AttributeValue.builder().n("2014").build());
        putItem(currentTableName, pixel2ItemAttributes);

        Thread.sleep(1000); // GSI's are eventually consistent

        Condition equals2012Condition = Condition.builder()
                .comparisonOperator(ComparisonOperator.EQ)
                .attributeValueList(
                    AttributeValue.builder()
                            .n("2012")
                            .build()
                )
                .build();

        QueryRequest equals2012Query = QueryRequest.builder()
                .tableName(currentTableName)
                .keyConditions(
                    Map.of(
                        YEAR, equals2012Condition
                    )
                )
                .indexName(YEAR_GSI_NAME)
                .build();

        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.query(equals2012Query)))
                .expectNextMatches(queryResponse ->
                    queryResponse.count() == 2
                        && queryResponse.items().stream().anyMatch(m -> m.get(COLOR).s().equals("Blue"))
                        && queryResponse.items().stream().anyMatch(m -> m.get(COLOR).s().equals("Silver"))
                )
                .verifyComplete();
    }

    @Test
    public void scanning() throws Exception {
        String currentTableName = "ScanTest";
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

        // scan everything, return everything
        ScanRequest scanEverythingRequest = ScanRequest.builder().tableName(currentTableName).build();

        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.scan(scanEverythingRequest)))
                .expectNextMatches(scanResponse -> scanResponse.scannedCount() == 3
                        && scanResponse.items().size() == 3
                )
                .verifyComplete();

        // scan everything, return just items with Color == "Cyan"
        ScanRequest scanForCyanRequest = ScanRequest.builder()
                .tableName(currentTableName)
                .filterExpression("Color = :color")
                .expressionAttributeValues(Map.of(":color", AttributeValue.builder().s("Cyan").build()))
                .build();

        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.scan(scanForCyanRequest)))
                .expectNextMatches(scanResponse -> scanResponse.scannedCount() == 3
                        && scanResponse.items().size() == 1
                        && scanResponse.items().get(0).get("Year").n().equals("2014")
                )
                .verifyComplete();
    }

    @Test
    public void nestedAttributes() throws Exception {
        String currentTableName = "NestedAttributesTest";
        createTableAndWaitForComplete(currentTableName);

        Map<String, AttributeValue> attributes = Map.of(
                COMPANY, AttributeValue.builder().s("Motorola").build(),
                MODEL, AttributeValue.builder().s("G1").build(),
                "MetadataList", AttributeValue.builder().l(
                        AttributeValue.builder().s("Super Cool").build(),
                        AttributeValue.builder().n("100").build()).build(),
                "MetadataStringSet", AttributeValue.builder().ss("one", "two", "three").build(),
                "MetadataNumberSet", AttributeValue.builder()
                        .bs(SdkBytes.fromByteArray(new byte[] {43, 123}), SdkBytes.fromByteArray(new byte[] {78, 100}))
                        .build()
            );

        PutItemRequest populateDataItemRequest = PutItemRequest.builder()
                .tableName(currentTableName)
                .item(attributes)
                .build();

        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.putItem(populateDataItemRequest)))
                .expectNextCount(1)
                .verifyComplete();

        GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName(currentTableName)
                .key(getMapWith("Motorola", "G1"))
                .build();

        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.getItem(getItemRequest)))
                .expectNextMatches(getItemResponse -> {
                    List<AttributeValue> listOfMetadata = getItemResponse.item().get("MetadataList").l();
                    List<String> stringSetMetadata = getItemResponse.item().get("MetadataStringSet").ss();

                    return listOfMetadata.size() == 2
                            && listOfMetadata.stream().anyMatch(attributeValue -> "Super Cool".equals(attributeValue.s()))
                            && listOfMetadata.stream().anyMatch(attributeValue -> "100".equals(attributeValue.n()))
                            && stringSetMetadata.contains("one")
                            && stringSetMetadata.contains("two");
                }).verifyComplete();
    }

    @Test
     public void transactions() throws Exception {
        String currentTableName = "TransactionsTest";
        createTableAndWaitForComplete(currentTableName);

        String partitionKey = "Google";
        String rangeKey1 = "Pixel 1";
        String rangeKey2 = "Future Phone";

        // create three items
        Map<String, AttributeValue> pixel1ItemAttributes = getMapWith(partitionKey, rangeKey1);
        pixel1ItemAttributes.put(COLOR, s("Blue"));
        pixel1ItemAttributes.put(YEAR, AttributeValue.builder().n("2012").build());
        putItem(currentTableName, pixel1ItemAttributes);

        Map<String, AttributeValue> futurePhoneAttributes = getMapWith(partitionKey, rangeKey2);
        futurePhoneAttributes.put(COLOR, s("Silver"));
        futurePhoneAttributes.put(YEAR, AttributeValue.builder().n("2030").build());
        putItem(currentTableName, futurePhoneAttributes);

        Map<String, AttributeValue> rangeKey1Map = Map.of(
            COMPANY, s(partitionKey),
            MODEL, s(rangeKey1)
        );

        TransactWriteItem updateColorToRedOnlyIfColorIsAlreadyBlue = TransactWriteItem.builder().update(
            Update.builder()
                .conditionExpression(COLOR + " = :color")
                .expressionAttributeValues(
                    Map.of(
                        ":color", s("Blue"),
                        ":newcolor", s("Red")
                    )
                )
                .tableName(currentTableName)
                .key(
                    rangeKey1Map
                )
                .updateExpression("SET " + COLOR + " = :newcolor")
                .build()
        ).build();

        TransactWriteItemsRequest updateColorOfItem1ToRedTransaction = TransactWriteItemsRequest.builder()
            .transactItems(
                updateColorToRedOnlyIfColorIsAlreadyBlue
            )
            .build();

        dynamoDbAsyncClient.transactWriteItems(updateColorOfItem1ToRedTransaction).get();

        CompletableFuture<GetItemResponse> getRangeKey1Future = dynamoDbAsyncClient.getItem(
            GetItemRequest.builder().key(rangeKey1Map).tableName(currentTableName).build()
        );

        StepVerifier.create(Mono.fromFuture(getRangeKey1Future))
                .expectNextMatches(getItemResponse -> getItemResponse.item().get(COLOR).s().equals("Red"))
                .verifyComplete();

        Map<String, AttributeValue> rangeKey2Map = Map.of(
            COMPANY, s(partitionKey),
            MODEL, s(rangeKey2)
        );

        TransactWriteItem updateRangeKey2ColorToOrange = TransactWriteItem.builder().update(
            Update.builder()
                .expressionAttributeValues(
                    Map.of(
                        ":newcolor", s("Orange")
                    )
                )
                .tableName(currentTableName)
                .key(
                    rangeKey1Map
                )
                .updateExpression("SET " + COLOR + " = :newcolor")
                .build()
        ).build();

        TransactWriteItemsRequest multiObjectTransactionThatShouldFailEverything = TransactWriteItemsRequest.builder()
            .transactItems(
                updateColorToRedOnlyIfColorIsAlreadyBlue,
                updateRangeKey2ColorToOrange
            )
            .build();

        StepVerifier.create(Mono.fromFuture(dynamoDbAsyncClient.transactWriteItems(multiObjectTransactionThatShouldFailEverything)))
                .expectErrorMatches(throwable -> {
                    List<CancellationReason> cancellationReasons =
                            ((TransactionCanceledException) throwable).cancellationReasons();
                    return cancellationReasons.get(0).code().equals("ConditionalCheckFailed");
                })
                .verify();

        CompletableFuture<GetItemResponse> getRangeKey2Future = dynamoDbAsyncClient.getItem(
            GetItemRequest.builder().key(rangeKey2Map).tableName(currentTableName).build()
        );

        // one operation (Blue -> Red) failed because of a condition check, therefore ALL operations fail
        StepVerifier.create(Mono.fromFuture(getRangeKey2Future))
            .expectNextMatches(getItemResponse ->
                !getItemResponse.item().get(COLOR).s().equals("Orange")
                    && getItemResponse.item().get(COLOR).s().equals("Silver")
            )
            .verifyComplete();
    }

    private AttributeValue s(String value) {
        return AttributeValue.builder().s(value).build();
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
