package com.example.neptune;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.neptunedata.NeptunedataClient;
import software.amazon.awssdk.services.neptunedata.model.ExecuteOpenCypherQueryRequest;
import software.amazon.awssdk.services.neptunedata.model.ExecuteOpenCypherQueryResponse;
import software.amazon.awssdk.services.neptunedata.model.GetEngineStatusRequest;
import software.amazon.awssdk.services.neptunedata.model.GetEngineStatusResponse;

import java.net.URI;
import java.util.Map;

/**
 * Demo application for connecting to Amazon Neptune using the Neptune Data API (REST)
 * This class performs the same operations as NeptuneBoltDemo but uses AWS SDK instead of Bolt driver
 */
public class NeptuneDataApiDemo {
    private static final Logger logger = LogManager.getLogger(NeptuneDataApiDemo.class);

    private final NeptunedataClient neptuneClient;
    private final String neptuneEndpoint;
    private final String awsRegion;

    public NeptuneDataApiDemo(NeptuneConfig config) {
        // Parse the URI to extract endpoint
        URI uri;
        try {
            uri = URI.create(config.getHttpsUri());
            this.neptuneEndpoint = config.getHost() + ":" + config.getPort();
            this.awsRegion = config.getRegion();
        } catch (Exception e) {
            throw new java.lang.IllegalArgumentException("Invalid Neptune URI: " + config.getHttpsUri(), e);
        }

        AwsCredentialsProvider credentialsProvider = config.isIamAuth() ?
                config.getCredentialsProvider() :
                AnonymousCredentialsProvider.create();

        // Create Neptune Data API client
        this.neptuneClient = NeptunedataClient.builder()
                .region(Region.of(config.getRegion()))
                .endpointOverride(uri)
                .credentialsProvider(credentialsProvider)
                .build();

        logger.info("Successfully created Neptune Data API client for endpoint: {}", neptuneEndpoint);
    }

    /**
     * Execute a simple query to test the connection
     */
    public void testConnection() {
        try {
            ExecuteOpenCypherQueryRequest request = ExecuteOpenCypherQueryRequest.builder()
                    .openCypherQuery("RETURN 'Hello Neptune!' as message")
                    .build();

            ExecuteOpenCypherQueryResponse response = neptuneClient.executeOpenCypherQuery(request);

            // Parse the response
            Document results = response.results();
            logger.info("Connection test successful. Response: {}", results.toString());

        } catch (Exception e) {
            logger.error("Connection test failed", e);
            throw new RuntimeException("Connection test failed", e);
        }
    }

    /**
     * Create sample nodes and relationships using OpenCypher
     */
    public void createSampleData() {
        try {
            // Create Person nodes
            executeQuery("CREATE (p:Person {name: 'Alice', age: 30})");
            executeQuery("CREATE (p:Person {name: 'Bob', age: 25})");
            executeQuery("CREATE (c:Company {name: 'TechCorp'})");

            // Create relationships
            executeQuery("MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'TechCorp'}) " +
                    "CREATE (a)-[:WORKS_FOR]->(c)");
            executeQuery("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) " +
                    "CREATE (a)-[:KNOWS]->(b)");

            logger.info("Sample data created successfully using Neptune Data API");
        } catch (Exception e) {
            logger.error("Failed to create sample data", e);
            throw e;
        }
    }

    /**
     * Query sample data using OpenCypher
     */
    public void querySampleData() {
        try {
            // Query all persons
            logger.info("Querying persons in the database:");
            //ExecuteOpenCypherQueryResponse response = executeQuery("MATCH (p:Person) RETURN p.name as name, p.age as age");
            ExecuteOpenCypherQueryResponse response = executeQuery("RETURN ST_GeomFromText(\"POINT (5 5)\");");

            Document results = response.results();
            logger.info("Persons query results: {}", results.toString());

            /*// Parse results if they contain a results array
            if (results.isMap() && results.asMap().containsKey("results")) {
                Document resultsArray = results.asMap().get("results");
                if (resultsArray.isList()) {
                    for (Document row : resultsArray.asList()) {
                        if (row.isMap()) {
                            Map<String, Document> rowMap = row.asMap();
                            String name = rowMap.containsKey("name") ? rowMap.get("name").asString() : "Unknown";
                            Integer age = rowMap.containsKey("age") ? rowMap.get("age").asNumber().intValue() : 0;
                            logger.info("- Name: {}, Age: {}", name, age);
                        }
                    }
                }
            }

            // Query relationships
            logger.info("Querying relationships:");
            response = executeQuery(
                    "MATCH (p1:Person)-[r]->(p2) " +
                            "RETURN p1.name as person1, type(r) as relationship, p2.name as person2"
            );

            results = response.results();
            logger.info("Relationships query results: {}", results.toString());

            // Parse relationship results
            if (results.isMap() && results.asMap().containsKey("results")) {
                Document resultsArray = results.asMap().get("results");
                if (resultsArray.isList()) {
                    for (Document row : resultsArray.asList()) {
                        if (row.isMap()) {
                            Map<String, Document> rowMap = row.asMap();
                            String person1 = rowMap.containsKey("person1") ? rowMap.get("person1").asString() : "Unknown";
                            String relationship = rowMap.containsKey("relationship") ? rowMap.get("relationship").asString() : "Unknown";
                            String person2 = rowMap.containsKey("person2") ? rowMap.get("person2").asString() : "Unknown";
                            logger.info("- {} {} {}", person1, relationship, person2);
                        }
                    }
                }
            }*/

        } catch (Exception e) {
            logger.error("Query execution failed", e);
            throw e;
        }
    }

    /**
     * Clean up sample data
     */
    public void cleanupSampleData() {
        try {
            executeQuery("MATCH (n) DETACH DELETE n");
            logger.info("Sample data cleaned up using Neptune Data API");
        } catch (Exception e) {
            logger.error("Failed to cleanup sample data", e);
            throw e;
        }
    }

    /**
     * Execute an OpenCypher query using Neptune Data API
     */
    private ExecuteOpenCypherQueryResponse executeQuery(String query) {
        try {
            ExecuteOpenCypherQueryRequest request = ExecuteOpenCypherQueryRequest.builder()
                    .openCypherQuery(query)
                    .build();

            ExecuteOpenCypherQueryResponse response = neptuneClient.executeOpenCypherQuery(request);
            logger.debug("Executed query: {}", query);

            return response;
        } catch (Exception e) {
            logger.error("Failed to execute query: {}", query, e);
            throw e;
        }
    }

    /**
     * Get Neptune cluster status
     */
    public void getClusterStatus() {
        try {
            GetEngineStatusRequest request = GetEngineStatusRequest.builder().build();
            GetEngineStatusResponse response = neptuneClient.getEngineStatus(request);

            logger.info("Neptune cluster status: {}", response.status());
            logger.info("Database engine: {}", response.dbEngineVersion());

        } catch (Exception e) {
            logger.error("Failed to get cluster status", e);
        }
    }

    /**
     * Close the Neptune client
     */
    public void close() {
        if (neptuneClient != null) {
            neptuneClient.close();
            logger.info("Neptune Data API client closed");
        }
    }

    public static void main(String[] args) {
        NeptuneConfig config = NeptuneConfig.fromProperties();

        logger.info("Connecting to Neptune Data API at: {}", config.getHost());
        logger.info("Neptune Port: {}", config.getPort());
        logger.info("AWS Region: {}", config.getRegion());

        NeptuneDataApiDemo demo = null;
        try {
            // Create connection
            demo = new NeptuneDataApiDemo(config);

            // Get cluster status
            demo.getClusterStatus();

            // Test connection
            demo.testConnection();

            // Create and query sample data
            //demo.createSampleData();
            demo.querySampleData();

            // Cleanup
            //demo.cleanupSampleData();

        } catch (Exception e) {
            logger.error("Application failed", e);
            System.exit(1);
        } finally {
            if (demo != null) {
                demo.close();
            }
        }

        logger.info("Neptune Data API Demo completed successfully");
    }
}
