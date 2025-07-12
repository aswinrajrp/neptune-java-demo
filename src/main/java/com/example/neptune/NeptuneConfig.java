package com.example.neptune;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class NeptuneConfig {
    private final String host;
    private final String port;
    private final String region;
    private final boolean iamAuth;
    private final String accessKey;
    private final String secretKey;
    private final String sessionToken;

    public NeptuneConfig(String host, String port, String region, boolean iamAuth, String accessKey, String secretKey, String sessionToken) {
        this.host = host;
        this.port = port;
        this.region = region;
        this.iamAuth = iamAuth;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.sessionToken = sessionToken;
    }

    public static NeptuneConfig fromProperties() {
        Properties properties = loadProperties();
        
        String uri = getConfigValue(properties, "neptune.endpoint", "NEPTUNE_ENDPOINT", null);
        String port = getConfigValue(properties, "neptune.port", "NEPTUNE_PORT", "8182");
        String region = getConfigValue(properties, "aws.region", "AWS_REGION", "us-east-1");
        boolean iamAuth = Boolean.parseBoolean(getConfigValue(properties, "neptune.iam.auth", "NEPTUNE_IAM_AUTH", "false"));
        String accessKey = getConfigValue(properties, "aws.access.key", "AWS_ACCESS_KEY_ID", null);
        String secretKey = getConfigValue(properties, "aws.secret.key", "AWS_SECRET_ACCESS_KEY", null);
        String sessionToken = getConfigValue(properties, "aws.session.token", "AWS_SESSION_TOKEN", null);
        
        return new NeptuneConfig(uri, port, region, iamAuth, accessKey, secretKey, sessionToken);
    }

    public AwsCredentialsProvider getCredentialsProvider() {
        if (accessKey != null && secretKey != null) {
            if (sessionToken != null) {
                // Use session credentials (temporary credentials)
                return StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(accessKey, secretKey, sessionToken)
                );
            } else {
                // Use basic credentials (long-term credentials)
                return StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey)
                );
            }
        } else {
            // Use default credential chain
            return DefaultCredentialsProvider.create();
        }
    }

    private static Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream input = NeptuneConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException e) {
            // Ignore
        }
        return properties;
    }

    private static String getConfigValue(Properties properties, String propertyKey, String envKey, String defaultValue) {
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.trim().isEmpty()) {
            return envValue.trim();
        }
        
        String propValue = properties.getProperty(propertyKey);
        if (propValue != null && !propValue.trim().isEmpty()) {
            return propValue.trim();
        }
        
        return defaultValue;
    }

    public String getHost() { return host; }
    public String getPort() { return port; }
    public String getRegion() { return region; }
    public boolean isIamAuth() { return iamAuth; }
    
    public String getBoltUri() {
        return "bolt://" + host + ":" + port;
    }
    
    public String getHttpsUri() {
        return "https://" + host + ":" + port;
    }
}
