package org.jboss.naming.remote.client;

import org.jboss.remoting3.Connection;

/**
 * Abstraction around the ejb client context, to get around class loading issues.
 *
 *
 * @author Stuart Douglas
 */
public final class CurrentEjbClientConnection {

    private volatile Connection connection;

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(final Connection connection) {
        this.connection = connection;
    }
}
