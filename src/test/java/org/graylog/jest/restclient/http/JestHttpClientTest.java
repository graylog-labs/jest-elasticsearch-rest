package org.graylog.jest.restclient.http;

import io.searchbox.core.Search;
import io.searchbox.core.search.sort.Sort;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpVersion;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.graylog.jest.restclient.JestClientFactory;
import org.graylog.jest.restclient.config.HttpClientConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Dogukan Sonmez
 */
public class JestHttpClientTest {
    JestHttpClient client;

    @Before
    public void init() {
        client = new JestHttpClient();
    }

    @After
    public void cleanup() {
        client = null;
    }

    @Test
    public void addHeadersToRequest() throws IOException {
        final String headerKey = "foo";
        final String headerValue = "bar";

        Response httpResponseMock = mock(Response.class);
        doReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK")).when(httpResponseMock).getStatusLine();
        doReturn(null).when(httpResponseMock).getEntity();

        RestClient restClientMock = mock(RestClient.class);
        doReturn(httpResponseMock).when(restClientMock).performRequest(anyString(), anyString(), anyMap(), any(HttpEntity.class), any(Header[].class));

        // Construct a new Jest client according to configuration via factory
        JestHttpClient clientWithMockedHttpClient;
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200").build());
        clientWithMockedHttpClient = (JestHttpClient) factory.getObject();
        clientWithMockedHttpClient.setRestClient(restClientMock);

        // could reuse the above setup for testing core types against expected
        // HttpUriRequest (more of an end to end test)

        String query = "{\n" +
                "    \"query\": {\n" +
                "        \"filtered\" : {\n" +
                "            \"query\" : {\n" +
                "                \"query_string\" : {\n" +
                "                    \"query\" : \"test\"\n" +
                "                }\n" +
                "            },\n" +
                "            \"filter\" : {\n" +
                "                \"term\" : { \"user\" : \"kimchy\" }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        Search search = new Search.Builder(query)
                // multiple index or types can be added.
                .addIndex("twitter")
                .addType("tweet")
                .setHeader(headerKey, headerValue)
                .build();
        // send request (not really)
        clientWithMockedHttpClient.execute(search);

        /* TODO Migrate verification
        verify(restClientMock).performRequest(argThat(new ArgumentMatcher<HttpUriRequest>() {
            @Override
            public boolean matches(Object o) {
                boolean retval = false;

                if (o instanceof HttpUriRequest) {
                    HttpUriRequest req = (HttpUriRequest) o;
                    Header header = req.getFirstHeader(headerKey);
                    if (header != null) {
                        retval = headerValue.equals(header.getValue());
                    }
                }

                return retval;
            }
        }));
        */
    }

    @SuppressWarnings ("unchecked")
    @Test
    public void prepareShouldNotRewriteLongToDoubles() throws IOException {
        // Construct a new Jest client according to configuration via factory
        JestHttpClient clientWithMockedHttpClient = (JestHttpClient) new JestClientFactory().getObject();

        // Construct mock Sort
        Sort mockSort = mock(Sort.class);

        String query = "{\n" +
                "    \"query\": {\n" +
                "        \"bool\" : {\n" +
                "            \"should\" : [\n" +
                "                { \"term\" : { \"id\" : 1234 } },\n" +
                "                { \"term\" : { \"id\" : 567800000000000000000 } }\n" +
                "            ]\n" +
                "         }\n" +
                "     }\n" +
                "}";
        Search search = new Search.Builder(query)
                // multiple index or types can be added.
                .addIndex("twitter")
                .addType("tweet")
                .addSort(mockSort)
                .build();

        // Create HttpUriRequest
        /* TODO: Migrate test
        String elasticSearchRestUrl = clientWithMockedHttpClient.getRequestURL(clientWithMockedHttpClient.getNextServer(), search.getURI());
        HttpUriRequest request1 = clientWithMockedHttpClient.constructHttpMethod(search.getRestMethodName(), elasticSearchRestUrl, search.getData(clientWithMockedHttpClient.getGson()));

        JestHttpClient.log.debug("Request method={} url={}", search.getRestMethodName(), elasticSearchRestUrl);

        // add headers added to action
        for (Map.Entry<String, Object> header : search.getHeaders().entrySet()) {
            request1.addHeader(header.getKey(), header.getValue().toString());
        }

        HttpUriRequest request = request1;

        // Extract Payload
        HttpEntity entity = ((HttpPost) request).getEntity();
        String payload = IOUtils.toString(entity.getContent(), Charset.defaultCharset());

        // Verify payload does not have a double
        assertFalse(payload.contains("1234.0"));
        assertTrue(payload.contains("1234"));

        // Verify payload does not use scientific notation
        assertFalse(payload.contains("5.678E20"));
        assertTrue(payload.contains("567800000000000000000"));
        */
    }

    @Test
    public void createContextInstanceWithPreemptiveAuth() {
        AuthCache authCacheMock = mock(AuthCache.class);
        CredentialsProvider credentialsProviderMock = mock(CredentialsProvider.class);

        HttpClientContext httpClientContextTemplate = HttpClientContext.create();
        httpClientContextTemplate.setAuthCache(authCacheMock);
        httpClientContextTemplate.setCredentialsProvider(credentialsProviderMock);

        JestHttpClient jestHttpClient = (JestHttpClient) new JestClientFactory().getObject();
        jestHttpClient.setHttpClientContextTemplate(httpClientContextTemplate);

        HttpClientContext httpClientContextResult = jestHttpClient.createContextInstance();

        assertEquals(authCacheMock, httpClientContextResult.getAuthCache());
        assertEquals(credentialsProviderMock, httpClientContextResult.getCredentialsProvider());
    }
}
