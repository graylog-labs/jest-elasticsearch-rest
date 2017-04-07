package org.graylog.jest.restclient.config.idle;

import io.searchbox.client.config.idle.ReapableConnectionManager;
import org.apache.http.nio.conn.NHttpClientConnectionManager;

import java.util.concurrent.TimeUnit;

public class HttpReapableConnectionManager implements ReapableConnectionManager {
    private final NHttpClientConnectionManager connectionManager;

    public HttpReapableConnectionManager(NHttpClientConnectionManager connectionManager) {
        if (connectionManager == null) throw new IllegalArgumentException();

        this.connectionManager = connectionManager;
    }

    @Override
    public void closeIdleConnections(long idleTimeout, TimeUnit unit) {
        connectionManager.closeIdleConnections(idleTimeout, unit);
    }
}
