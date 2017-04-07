package org.graylog.jest.restclient;

import com.google.gson.Gson;
import io.searchbox.client.JestClient;
import io.searchbox.client.config.discovery.NodeChecker;
import io.searchbox.client.config.idle.IdleConnectionReaper;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.AuthCache;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.routing.HttpRoutePlanner;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.reactor.IOReactorException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.graylog.jest.restclient.config.HttpClientConfig;
import org.graylog.jest.restclient.config.idle.HttpReapableConnectionManager;
import org.graylog.jest.restclient.http.JestHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * @author Dogukan Sonmez
 */
public class JestClientFactory {

    private static final Logger log = LoggerFactory.getLogger(JestClientFactory.class);
    private HttpClientConfig httpClientConfig;

    public JestClient getObject() {
        JestHttpClient client = new JestHttpClient();

        if (httpClientConfig == null) {
            log.debug("There is no configuration to create http client. Going to create simple client with default values");
            httpClientConfig = new HttpClientConfig.Builder("http://localhost:9200").build();
        }

        client.setRequestCompressionEnabled(httpClientConfig.isRequestCompressionEnabled());
        client.setServers(httpClientConfig.getServerList());
        final NHttpClientConnectionManager connectionManager = getAsyncConnectionManager();
        client.setRestClient(createRestClient(connectionManager));

        // set custom gson instance
        Gson gson = httpClientConfig.getGson();
        if (gson == null) {
            log.info("Using default GSON instance");
        } else {
            log.info("Using custom GSON instance");
            client.setGson(gson);
        }

        // set discovery (should be set after setting the httpClient on jestClient)
        if (httpClientConfig.isDiscoveryEnabled()) {
            log.info("Node Discovery enabled...");
            if (StringUtils.isNotEmpty(httpClientConfig.getDiscoveryFilter())) {
                log.info("Node Discovery filtering nodes on \"{}\"", httpClientConfig.getDiscoveryFilter());
            }
            NodeChecker nodeChecker = createNodeChecker(client, httpClientConfig);
            client.setNodeChecker(nodeChecker);
            nodeChecker.startAsync();
            nodeChecker.awaitRunning();
        } else {
            log.info("Node Discovery disabled...");
        }

        // schedule idle connection reaping if configured
        if (httpClientConfig.getMaxConnectionIdleTime() > 0) {
            log.info("Idle connection reaping enabled...");

            IdleConnectionReaper reaper = new IdleConnectionReaper(httpClientConfig, new HttpReapableConnectionManager(connectionManager));
            client.setIdleConnectionReaper(reaper);
            reaper.startAsync();
            reaper.awaitRunning();
        } else {
            log.info("Idle connection reaping disabled...");
        }

        Set<HttpHost> preemptiveAuthTargetHosts = httpClientConfig.getPreemptiveAuthTargetHosts();
        if (!preemptiveAuthTargetHosts.isEmpty()) {
            log.info("Authentication cache set for preemptive authentication");
            client.setHttpClientContextTemplate(createPreemptiveAuthContext(preemptiveAuthTargetHosts));
        }

        return client;
    }

    public void setHttpClientConfig(HttpClientConfig httpClientConfig) {
        this.httpClientConfig = httpClientConfig;
    }

    private RestClient createRestClient(NHttpClientConnectionManager connectionManager) {
        final HttpHost[] initialHttpHosts = httpClientConfig.getServerList().stream().map(HttpHost::create).toArray(HttpHost[]::new);
        return configureRestClient(
                RestClient.builder(initialHttpHosts)
                        .setHttpClientConfigCallback(builder -> configureAsyncClient(builder
                                .setConnectionManager(connectionManager)
                                .setDefaultRequestConfig(getRequestConfig())
                                .setProxyAuthenticationStrategy(httpClientConfig.getProxyAuthenticationStrategy())
                                .setRoutePlanner(getRoutePlanner())
                                .setDefaultCredentialsProvider(httpClientConfig.getCredentialsProvider())))
                        .setRequestConfigCallback(this::configureRequestConfig)
        ).build();
    }


    /**
     * Extension point
     * <p>
     * Example:
     * </p>
     * <pre>
     * final JestClientFactory factory = new JestClientFactory() {
     *    {@literal @Override}
     *  	protected RestClientBuilder configureRestClient(RestClientBuilder builder) {
     *  		return builder.setHttpClientConfigCallback(...);
     *    }
     * }
     * </pre>
     */
    protected RestClientBuilder configureRestClient(final RestClientBuilder builder) {
        return builder;
    }

    // Extension point
    protected HttpAsyncClientBuilder configureAsyncClient(final HttpAsyncClientBuilder builder) {
        return builder;
    }

    // Extension point
    protected RequestConfig.Builder configureRequestConfig(final RequestConfig.Builder builder) {
        return builder;
    }

    // Extension point
    protected HttpRoutePlanner getRoutePlanner() {
        return httpClientConfig.getHttpRoutePlanner();
    }

    // Extension point
    protected RequestConfig getRequestConfig() {
        return RequestConfig.custom()
                .setConnectTimeout(httpClientConfig.getConnTimeout())
                .setSocketTimeout(httpClientConfig.getReadTimeout())
                .build();
    }

    // Extension point
    protected NHttpClientConnectionManager getAsyncConnectionManager() {
        PoolingNHttpClientConnectionManager retval;

        IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                .setConnectTimeout(httpClientConfig.getConnTimeout())
                .setSoTimeout(httpClientConfig.getReadTimeout())
                .build();

        Registry<SchemeIOSessionStrategy> sessionStrategyRegistry = RegistryBuilder.<SchemeIOSessionStrategy>create()
                .register("http", httpClientConfig.getHttpIOSessionStrategy())
                .register("https", httpClientConfig.getHttpsIOSessionStrategy())
                .build();

        try {
            retval = new PoolingNHttpClientConnectionManager(
                    new DefaultConnectingIOReactor(ioReactorConfig),
                    sessionStrategyRegistry
            );
        } catch (IOReactorException e) {
            throw new IllegalStateException(e);
        }

        final Integer maxTotal = httpClientConfig.getMaxTotalConnection();
        if (maxTotal != null) {
            retval.setMaxTotal(maxTotal);
        }
        final Integer defaultMaxPerRoute = httpClientConfig.getDefaultMaxTotalConnectionPerRoute();
        if (defaultMaxPerRoute != null) {
            retval.setDefaultMaxPerRoute(defaultMaxPerRoute);
        }
        final Map<HttpRoute, Integer> maxPerRoute = httpClientConfig.getMaxTotalConnectionPerRoute();
        for (Map.Entry<HttpRoute, Integer> entry : maxPerRoute.entrySet()) {
            retval.setMaxPerRoute(entry.getKey(), entry.getValue());
        }

        return retval;
    }

    // Extension point
    protected NodeChecker createNodeChecker(JestHttpClient client, HttpClientConfig httpClientConfig) {
        return new NodeChecker(client, httpClientConfig);
    }

    // Extension point
    protected HttpClientContext createPreemptiveAuthContext(Set<HttpHost> targetHosts) {
        HttpClientContext context = HttpClientContext.create();
        context.setCredentialsProvider(httpClientConfig.getCredentialsProvider());
        context.setAuthCache(createBasicAuthCache(targetHosts));

        return context;
    }

    private AuthCache createBasicAuthCache(Set<HttpHost> targetHosts) {
        AuthCache authCache = new BasicAuthCache();
        BasicScheme basicAuth = new BasicScheme();
        for (HttpHost eachTargetHost : targetHosts) {
            authCache.put(eachTargetHost, basicAuth);
        }

        return authCache;
    }

}
