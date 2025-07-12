package com.example.neptune;

import com.google.gson.Gson;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.value.StringValue;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.neo4j.driver.internal.security.InternalAuthToken.CREDENTIALS_KEY;
import static org.neo4j.driver.internal.security.InternalAuthToken.PRINCIPAL_KEY;
import static org.neo4j.driver.internal.security.InternalAuthToken.REALM_KEY;
import static org.neo4j.driver.internal.security.InternalAuthToken.SCHEME_KEY;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.AUTHORIZATION;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.HOST;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.X_AMZ_DATE;
import static software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerConstant.X_AMZ_SECURITY_TOKEN;

/**
 * Use this class instead of `AuthTokens.basic` when working with an IAM
 * auth-enabled server. It works the same as `AuthTokens.basic` when using
 * static credentials, and avoids making requests with an expired signature
 * when using temporary credentials. Internally, it generates a new signature
 * on every invocation (this may change in a future implementation).
 * <p>
 * Note that authentication happens only the first time for a pooled connection.
 * <p>
 * Typical usage:
 *
 * <pre>
 * AuthToken authToken = new NeptuneAuthToken(
 *     "aws-region",
 *     "cluster-endpoint-url",
 *     credentialsProvider
 * ).toAuthToken();
 *
 * Driver driver = GraphDatabase.driver(
 *     authToken.getUrl(),
 *     authToken,
 *     config
 * );
 * </pre>
 */
public class NeptuneAuthToken {
    private static final String SCHEME = "basic";
    private static final String REALM = "realm";
    private static final String SERVICE_NAME = "neptune-db";
    private static final String HTTP_METHOD_HDR = "HttpMethod";
    private static final String DUMMY_USERNAME = "username";

    private final String region;
    private final String url;
    private final AwsCredentialsProvider credentialsProvider;
    private final Gson gson = new Gson();

    public NeptuneAuthToken(String region, String url, AwsCredentialsProvider credentialsProvider) {
        // The superclass caches the result of toMap(), which we don't want
        this.region = region;
        this.url = url;
        this.credentialsProvider = credentialsProvider;
    }

    public AuthToken toAuthToken() {
        return new InternalAuthToken(toMap());
    }

    public String getUrl() {
        return url;
    }

    private Map<String, Value> toMap() {
        final Map<String, Value> map = new HashMap<>();
        map.put(SCHEME_KEY, Values.value(SCHEME));
        map.put(PRINCIPAL_KEY, Values.value(DUMMY_USERNAME));
        map.put(CREDENTIALS_KEY, new StringValue(getSignedHeader()));
        map.put(REALM_KEY, Values.value(REALM));

        return map;
    }

    private String getSignedHeader() {
        URI uri = URI.create(url);

        SdkHttpFullRequest request = SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.GET)
                .uri(uri)
                .protocol(uri.getScheme())
                .appendRawQueryParameter("", "")
                .encodedPath("/opencypher")
                .build();

        Aws4SignerParams signerParams = Aws4SignerParams.builder()
                .awsCredentials(credentialsProvider.resolveCredentials())
                .signingName(SERVICE_NAME)
                .signingRegion(Region.of(region))
                .build();

        Aws4Signer signer = Aws4Signer.create();
        SdkHttpFullRequest signedRequest = signer.sign(request, signerParams);

        return getAuthInfoJson(signedRequest);
    }

    private String getAuthInfoJson(SdkHttpFullRequest request) {
        final Map<String, Object> obj = new HashMap<>();
        obj.put(AUTHORIZATION, request.firstMatchingHeader(AUTHORIZATION).orElse(null));
        obj.put(HTTP_METHOD_HDR, request.method().name());
        obj.put(X_AMZ_DATE, request.firstMatchingHeader(X_AMZ_DATE).orElse(null));
        obj.put(HOST, request.firstMatchingHeader(HOST).orElse(null));
        obj.put(X_AMZ_SECURITY_TOKEN, request.firstMatchingHeader(X_AMZ_SECURITY_TOKEN).orElse(null));

        return gson.toJson(obj);
    }
}
