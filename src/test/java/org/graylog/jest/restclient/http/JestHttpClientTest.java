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
import org.graylog.jest.restclient.JestClientFactory;
import org.graylog.jest.restclient.config.HttpClientConfig;
import org.graylog.jest.restclient.http.apache.HttpDeleteWithEntity;
import org.graylog.jest.restclient.http.apache.HttpGetWithEntity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
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
    public void constructGetHttpMethod() throws UnsupportedEncodingException {
        HttpUriRequest request = client.constructHttpMethod("GET", "jest/get",
                null);
        assertNotNull(request);
        assertEquals(request.getURI().getPath(), "jest/get");
        assertTrue(request instanceof HttpGetWithEntity);
    }

    @Test
    public void constructCompressedPutHttpMethod() throws UnsupportedEncodingException {
        client.setRequestCompressionEnabled(true);

        HttpUriRequest request = client.constructHttpMethod("PUT", "jest/put", "data");
        assertNotNull(request);
        assertEquals(request.getURI().getPath(), "jest/put");
        assertTrue(request instanceof HttpPut);
        assertTrue(((HttpPut) request).getEntity() instanceof GzipCompressingEntity);
    }

    @Test
    public void constructPutHttpMethod() throws UnsupportedEncodingException {
        HttpUriRequest request = client.constructHttpMethod("PUT", "jest/put", "data");
        assertNotNull(request);
        assertEquals(request.getURI().getPath(), "jest/put");
        assertTrue(request instanceof HttpPut);
        assertFalse(((HttpPut) request).getEntity() instanceof GzipCompressingEntity);
    }

    @Test
    public void constructPostHttpMethod() throws UnsupportedEncodingException {
        HttpUriRequest request = client.constructHttpMethod("POST", "jest/post", "data");
        assertNotNull(request);
        assertEquals(request.getURI().getPath(), "jest/post");
        assertTrue(request instanceof HttpPost);
    }

    @Test
    public void constructDeleteHttpMethod() throws UnsupportedEncodingException {
        HttpUriRequest request = client.constructHttpMethod("DELETE", "jest/delete", null);
        assertNotNull(request);
        assertEquals(request.getURI().getPath(), "jest/delete");
        assertTrue(request instanceof HttpDeleteWithEntity);
    }

    @Test
    public void constructHeadHttpMethod() throws UnsupportedEncodingException {
        HttpUriRequest request = client.constructHttpMethod("HEAD", "jest/head", null);
        assertNotNull(request);
        assertEquals(request.getURI().getPath(), "jest/head");
        assertTrue(request instanceof HttpHead);
    }

    @Test
    public void addHeadersToRequest() throws IOException {
        final String headerKey = "foo";
        final String headerValue = "bar";

        CloseableHttpResponse httpResponseMock = mock(CloseableHttpResponse.class);
        doReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK")).when(httpResponseMock).getStatusLine();
        doReturn(null).when(httpResponseMock).getEntity();

        CloseableHttpClient closeableHttpClientMock = mock(CloseableHttpClient.class);
        doReturn(httpResponseMock).when(closeableHttpClientMock).execute(any(HttpUriRequest.class));

        // Construct a new Jest client according to configuration via factory
        JestHttpClient clientWithMockedHttpClient;
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200").build());
        clientWithMockedHttpClient = (JestHttpClient) factory.getObject();
        clientWithMockedHttpClient.setHttpClient(closeableHttpClientMock);

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

        verify(closeableHttpClientMock).execute(argThat(new ArgumentMatcher<HttpUriRequest>() {
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
        HttpUriRequest request = clientWithMockedHttpClient.prepareRequest(search);

        // Extract Payload
        HttpEntity entity = ((HttpPost) request).getEntity();
        String payload = IOUtils.toString(entity.getContent(), Charset.defaultCharset());

        // Verify payload does not have a double
        assertFalse(payload.contains("1234.0"));
        assertTrue(payload.contains("1234"));

        // Verify payload does not use scientific notation
        assertFalse(payload.contains("5.678E20"));
        assertTrue(payload.contains("567800000000000000000"));
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
