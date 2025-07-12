package com.example.neptune;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Main entry point for Neptune Java Demo application.
 * Runs either NeptuneBoltDemo or NeptuneDataApiDemo based on command-line parameter.
 * 
 * Usage:
 *   java -cp target/neptune-java-demo-1.0-SNAPSHOT.jar com.example.neptune.NeptuneDemo bolt
 *   java -cp target/neptune-java-demo-1.0-SNAPSHOT.jar com.example.neptune.NeptuneDemo data-api
 */
public class NeptuneDemo {
    private static final Logger logger = LogManager.getLogger(NeptuneDemo.class);
    
    private static final String USAGE = 
            "Usage: java -cp target/neptune-java-demo-1.0-SNAPSHOT.jar com.example.neptune.NeptuneDemo <demo-type>\n" +
            "\n" +
            "Demo Types:\n" +
            "  bolt      - Run Neptune demo using Bolt driver with Bolt protocol\n" +
            "  data-api  - Run Neptune demo using AWS SDK Neptune Data API (REST)\n" +
            "\n" +
            "Examples:\n" +
            "  java -cp target/neptune-java-demo-1.0-SNAPSHOT.jar com.example.neptune.NeptuneDemo bolt\n" +
            "  java -cp target/neptune-java-demo-1.0-SNAPSHOT.jar com.example.neptune.NeptuneDemo data-api\n";

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Error: Exactly one argument required.");
            System.err.println(USAGE);
            System.exit(1);
        }

        String demoType = args[0].toLowerCase();
        
        try {
            switch (demoType) {
                case "bolt":
                case "neo4j": // Keep backward compatibility
                    logger.info("Starting Neptune Bolt Demo (Bolt protocol)");
                    NeptuneBoltDemo.main(new String[0]);
                    break;
                    
                case "data-api":
                    logger.info("Starting Neptune Data API Demo (REST)");
                    NeptuneDataApiDemo.main(new String[0]);
                    break;
                    
                default:
                    System.err.println("Error: Invalid demo type '" + demoType + "'");
                    System.err.println("Valid options are: bolt, data-api");
                    System.err.println();
                    System.err.println(USAGE);
                    System.exit(1);
            }
        } catch (Exception e) {
            logger.error("Error running demo: " + e.getMessage(), e);
            System.err.println("Error running demo: " + e.getMessage());
            System.exit(1);
        }
    }
}
