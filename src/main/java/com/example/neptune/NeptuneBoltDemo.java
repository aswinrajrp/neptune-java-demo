package com.example.neptune;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.Neo4jException;

/**
 * Demo application for connecting to Amazon Neptune using OpenCypher via Bolt protocol
 */
public class NeptuneBoltDemo {
    private static final Logger logger = LogManager.getLogger(NeptuneBoltDemo.class);

    private final Driver driver;

    public NeptuneBoltDemo(NeptuneConfig config) {
        AuthToken authToken = config.isIamAuth() ?
                new NeptuneAuthToken(config.getRegion(), config.getHttpsUri(), config.getCredentialsProvider())
                        .toAuthToken() :
                AuthTokens.none();


        // Create driver instance
        driver = GraphDatabase.driver(config.getBoltUri(), authToken,
                Config.builder().withEncryption()
                        .withTrustStrategy(Config.TrustStrategy.trustSystemCertificates())
                        .build());

        logger.info("Successfully created Bolt driver for URI: {}", config.getBoltUri());
    }

    /**
     * Execute a simple query to test the connection
     */
    public void testConnection() {
        try (Session session = driver.session()) {
            // Simple query to test connectivity
            String query = "RETURN 'Hello from Neptune!' as message";

            logger.info("Executing test query: {}", query);
            Result result = session.run(query);

            if (result.hasNext()) {
                Record record = result.next();
                String message = record.get("message").asString();
                logger.info("Connection test successful: {}", message);
            } else {
                logger.warn("Query executed but returned no results");
            }

        } catch (Neo4jException e) {
            logger.error("Bolt driver error during test query: {}", e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected error during test query: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Execute sample OpenCypher queries to demonstrate Neptune functionality
     */
    public void runSampleQueries() {
        try (Session session = driver.session()) {
            logger.info("Starting sample OpenCypher queries");

            //String testQuery = "MATCH (n:Person) RETURN n.name";
            //String testQuery = "MATCH (n) RETURN n";
            String testQuery = "RETURN ST_AsText(ST_GeomFromText(\"POINT (5 5)\"))";

            Result queryResult = session.run(testQuery);

            while (queryResult.hasNext()) {
                Record record = queryResult.next();
                logger.info(record.toString());
            }

            /*// Query 1: Create some sample nodes
            String createQuery = """
                    CREATE (p1:Person {name: 'Alice', age: 30})
                    CREATE (p2:Person {name: 'Bob', age: 25})
                    CREATE (c:Company {name: 'TechCorp'})
                    CREATE (p1)-[:WORKS_FOR]->(c)
                    CREATE (p2)-[:WORKS_FOR]->(c)
                    RETURN p1.name as person1, p2.name as person2, c.name as company
                    """;

            logger.info("Creating sample data");
            Result createResult = session.run(createQuery);

            if (createResult.hasNext()) {
                Record record = createResult.next();
                logger.info("Created: {} and {} working for {}",
                        record.get("person1").asString(),
                        record.get("person2").asString(),
                        record.get("company").asString());
            }

            // Query 2: Find all persons
            String findQuery = "MATCH (p:Person) RETURN p.name as name, p.age as age ORDER BY p.name";

            logger.info("Finding all persons");
            Result findResult = session.run(findQuery);

            while (findResult.hasNext()) {
                Record record = findResult.next();
                logger.info("Person: {} (age: {})",
                        record.get("name").asString(),
                        record.get("age").asInt());
            }

            // Query 3: Find relationships
            String relationQuery = """
                    MATCH (p:Person)-[r:WORKS_FOR]->(c:Company)
                    RETURN p.name as person, type(r) as relationship, c.name as company
                    """;

            logger.info("Finding relationships");
            Result relationResult = session.run(relationQuery);

            while (relationResult.hasNext()) {
                Record record = relationResult.next();
                logger.info("{} {} {}",
                        record.get("person").asString(),
                        record.get("relationship").asString(),
                        record.get("company").asString());
            }

            // Query 4: Cleanup - remove the test data
            String cleanupQuery = """
                    MATCH (n)
                    WHERE n:Person OR n:Company
                    DETACH DELETE n
                    """;

            logger.info("Cleaning up test data");
            session.run(cleanupQuery);
            logger.info("Test data cleaned up successfully");*/

            logger.info("Sample queries completed successfully");

        } catch (Neo4jException e) {
            logger.error("Bolt driver error during sample queries: {}", e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected error during sample queries: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Close the driver connection
     */
    public void close() {
        if (driver != null) {
            driver.close();
            logger.info("Bolt driver closed");
        }
    }

    public static void main(String[] args) {
        NeptuneConfig config = NeptuneConfig.fromProperties();

        logger.info("Connecting to Neptune at: {}", config.getBoltUri());

        NeptuneBoltDemo demo = null;
        try {
            // Create connection
            demo = new NeptuneBoltDemo(config);

            // Test connection
            demo.testConnection();

            // Run sample queries
            demo.runSampleQueries();

        } catch (Exception e) {
            logger.error("Demo failed: {}", e.getMessage(), e);
            System.exit(1);
        } finally {
            if (demo != null) {
                demo.close();
            }
        }

        logger.info("Neptune Bolt Demo completed successfully");
    }
}
