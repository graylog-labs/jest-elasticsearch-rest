package org.graylog.jest.restclient.http;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import io.searchbox.action.Action;
import io.searchbox.client.AbstractJestClient;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.client.config.exception.CouldNotConnectException;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Dogukan Sonmez
 * @author cihat keser
 */
public class JestHttpClient extends AbstractJestClient {

    protected static final Logger log = LoggerFactory.getLogger(JestHttpClient.class);

    protected ContentType requestContentType = ContentType.APPLICATION_JSON.withCharset(StandardCharsets.UTF_8);

    private RestClient restClient;

    private HttpClientContext httpClientContextTemplate;

    /**
     * @throws IOException              in case of a problem or the connection was aborted during request,
     *                                  or in case of a problem while reading the response stream
     * @throws CouldNotConnectException if an {@link HttpHostConnectException} is encountered
     */
    @Override
    public <T extends JestResult> T execute(Action<T> clientRequest) throws IOException {
        final String elasticSearchRestUrl = getRequestURL("", clientRequest.getURI()); // getRequestURL(getNextServer(), clientRequest.getURI());
        final String methodName = clientRequest.getRestMethodName();
        log.debug("Request method={} url={}", methodName, elasticSearchRestUrl);

        final List<Header> headers = constructHeaders(clientRequest.getHeaders());
        final String payload = clientRequest.getData(gson);
        final HttpEntity entity = constructEntity(payload);

        try {
            final Response response = restClient.performRequest(
                    methodName,
                    elasticSearchRestUrl,
                    Collections.emptyMap(),
                    entity,
                    headers.toArray(new Header[0])
            );
            return deserializeResponse(response, clientRequest);
        } catch (HttpHostConnectException ex) {
            throw new CouldNotConnectException(ex.getHost().toURI(), ex);
        } catch (ResponseException ex) {
            log.debug("Request failed", ex);
            return deserializeResponse(ex.getResponse(), clientRequest);
        }
    }

    private HttpEntity constructEntity(String payload) {
        final HttpEntity entity;
        if (payload != null) {
            EntityBuilder entityBuilder = EntityBuilder.create()
                    .setText(payload)
                    .setContentType(requestContentType);

            if (isRequestCompressionEnabled()) {
                entityBuilder.gzipCompress();
            }

            entity = entityBuilder.build();
        } else {
            entity = null;
        }

        return entity;
    }

    private List<Header> constructHeaders(Map<String, Object> clientRequestHeaders) {
        final List<Header> headers = new ArrayList<>(clientRequestHeaders.size());
        for (Entry<String, Object> header : clientRequestHeaders.entrySet()) {
            headers.add(new BasicHeader(header.getKey(), header.getValue().toString()));
        }
        return headers;
    }

    @Override
    public <T extends JestResult> void executeAsync(final Action<T> clientRequest, final JestResultHandler<? super T> resultHandler) {
        final String elasticSearchRestUrl = getRequestURL("", clientRequest.getURI()); // getRequestURL(getNextServer(), clientRequest.getURI());
        final String methodName = clientRequest.getRestMethodName();
        log.debug("Request method={} url={}", methodName, elasticSearchRestUrl);

        final List<Header> headers = constructHeaders(clientRequest.getHeaders());
        final String payload = clientRequest.getData(gson);
        final HttpEntity entity = constructEntity(payload);

        restClient.performRequestAsync(
                methodName,
                elasticSearchRestUrl,
                Collections.emptyMap(),
                entity,
                new DefaultResponseListener<>(clientRequest, resultHandler),
                headers.toArray(new Header[0])
        );
    }

    @Override
    public void shutdownClient() {
        super.shutdownClient();
        try {
            restClient.close();
        } catch (IOException ex) {
            log.error("Exception occurred while shutting down the REST client.", ex);
        }
    }

    // TODO: Find out how to use this with the Elasticsearch low-level REST client
    protected HttpClientContext createContextInstance() {
        HttpClientContext context = HttpClientContext.create();
        context.setCredentialsProvider(httpClientContextTemplate.getCredentialsProvider());
        context.setAuthCache(httpClientContextTemplate.getAuthCache());

        return context;
    }

    private <T extends JestResult> T deserializeResponse(Response response, Action<T> clientRequest) throws IOException {
        StatusLine statusLine = response.getStatusLine();
        try {
            return clientRequest.createNewElasticSearchResult(
                    response.getEntity() == null ? null : EntityUtils.toString(response.getEntity()),
                    statusLine.getStatusCode(),
                    statusLine.getReasonPhrase(),
                    gson
            );
        } catch (com.google.gson.JsonSyntaxException e) {
            String mimeType = response.getHeader("Content-Type");
            if (!mimeType.startsWith("application/json")) {
                // probably a proxy that responded in text/html
                final String message = "Request yielded " + mimeType + ", should be json: " + statusLine.toString();
                throw new IOException(message, e);
            }

            throw e;
        }
    }

    @Override
    public void setServers(Set<String> servers) {
        super.setServers(servers);

        if(restClient != null) {
            final HttpHost[] hosts = servers.stream().map(HttpHost::create).toArray(HttpHost[]::new);
            restClient.setHosts(hosts);
        }
    }

    public RestClient getRestClient() {
        return restClient;
    }

    public void setRestClient(RestClient restClient) {
        this.restClient = restClient;
    }

    public Gson getGson() {
        return gson;
    }

    public void setGson(Gson gson) {
        this.gson = gson;
    }

    @Deprecated
    public HttpClientContext getHttpClientContextTemplate() {
        return httpClientContextTemplate;
    }

    @Deprecated
    public void setHttpClientContextTemplate(HttpClientContext httpClientContext) {
        this.httpClientContextTemplate = httpClientContext;
    }

    @Override
    @VisibleForTesting
    public int getServerPoolSize() {
        return super.getServerPoolSize();
    }

    @Override
    @VisibleForTesting
    public String getNextServer() {
        return super.getNextServer();
    }

    protected class DefaultResponseListener<T extends JestResult> implements ResponseListener {
        private final Action<T> clientRequest;
        private final JestResultHandler<? super T> resultHandler;

        public DefaultResponseListener(Action<T> clientRequest, JestResultHandler<? super T> resultHandler) {
            this.clientRequest = clientRequest;
            this.resultHandler = resultHandler;
        }

        @Override
        public void onSuccess(Response response) {
            T jestResult = null;
            try {
                jestResult = deserializeResponse(response, clientRequest);
            } catch (Exception e) {
                onFailure(e);
            } catch (Throwable t) {
                onFailure(new Exception("Problem during request processing", t));
            }
            if (jestResult != null) {
                resultHandler.completed(jestResult);
            }
        }

        @Override
        public void onFailure(Exception ex) {
            log.error("Exception occurred during async execution.", ex);
            if (ex instanceof HttpHostConnectException) {
                String host = ((HttpHostConnectException) ex).getHost().toURI();
                resultHandler.failed(new CouldNotConnectException(host, ex));
                return;
            }
            resultHandler.failed(ex);
        }
    }
}
