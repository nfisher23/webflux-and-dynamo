package com.nickolasfisher.reactivedynamo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class DynamoTestContainerTest {

    public static final String COMPANY = "Company";
    public static final String MODEL = "Model";

    private static DynamoDbAsyncClient dynamoDbAsyncClient;

    @Container
    public static GenericContainer genericContainer = new GenericContainer(
            DockerImageName.parse("amazon/dynamodb-local")
    ).withExposedPorts(8000);

    @BeforeEach
    public void setupDynamoClient() {
        dynamoDbAsyncClient = getDynamoClient();
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
                .endpointOverride(URI.create("http://localhost:" + genericContainer.getFirstMappedPort()))
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

}
