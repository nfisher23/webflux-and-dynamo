package com.nickolasfisher.reactivedynamo;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

public class PhoneServiceTest {

    private static DynamoDBProxyServer dynamoProxy;

    private static int port;

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
        DynamoDbAsyncClient client = DynamoDbAsyncClient.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create("http://localhost:" + port))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("FAKE", "FAKE")))
                .build();

        ListTablesResponse listTablesResponse = client.listTables().get();

        assertThat(listTablesResponse.tableNames().size()).isEqualTo(0);

        client.createTable(CreateTableRequest.builder()
                .keySchema(
                        KeySchemaElement.builder().keyType(KeyType.HASH).attributeName("Company").build(),
                        KeySchemaElement.builder().keyType(KeyType.RANGE).attributeName("Model").build()
                )
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("Company").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("Model").attributeType(ScalarAttributeType.S).build()
                )
                .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(100L).writeCapacityUnits(100L).build())
                .tableName("Phones")
                .build())
                .get();

        ListTablesResponse listTablesResponseAfterCreation = client.listTables().get();

        assertThat(listTablesResponseAfterCreation.tableNames().size()).isEqualTo(1);
    }
}
