package io.searchbox.common;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.graylog.jest.restclient.JestClientFactory;
import org.graylog.jest.restclient.config.HttpClientConfig;
import org.graylog.jest.restclient.http.JestHttpClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

/**
 * @author Dogukan Sonmez
 */
@Ignore
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public abstract class AbstractIntegrationTest extends ESIntegTestCase {

    protected final JestClientFactory factory = new JestClientFactory();
    protected JestHttpClient client;

    protected int getPort() {
        assertTrue("There should be at least 1 HTTP endpoint exposed in the test cluster",
                cluster().httpAddresses().length > 0);
        return cluster().httpAddresses()[0].getPort();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(Node.HTTP_ENABLED, true)
                .build();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        factory.setHttpClientConfig(
                new HttpClientConfig
                        .Builder("http://localhost:" + getPort())
                        .multiThreaded(true).build()
        );
        client = (JestHttpClient) factory.getObject();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        client.shutdownClient();
        client = null;
    }

}
