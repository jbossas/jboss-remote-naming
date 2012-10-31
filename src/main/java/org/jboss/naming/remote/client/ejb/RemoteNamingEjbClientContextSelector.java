package org.jboss.naming.remote.client.ejb;

import org.jboss.ejb.client.ContextSelector;
import org.jboss.ejb.client.EJBClientContext;
import org.jboss.naming.remote.client.CurrentEjbClientConnection;
import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.Connection;

import java.io.IOException;
import java.util.IdentityHashMap;

/**
 * @author John Bailey
 * @deprecated Since 1.0.6.Final - remote-naming no longer switches EJB client context selector for providing
 *              EJB invocation support. Instead we use {@link EJBClientHandler} to handle EJB integration in remote-naming
 */
@Deprecated
public class RemoteNamingEjbClientContextSelector implements ContextSelector<EJBClientContext> {

    private ContextSelector<EJBClientContext> delegate;
    private final ThreadLocal<CurrentEjbClientConnection> currentContext = new ThreadLocal<CurrentEjbClientConnection>();

    /**
     * Map of receiver by connection
     */
    private final IdentityHashMap<Connection, EJBClientContext> contextByConnection = new IdentityHashMap<Connection, EJBClientContext>();


    public synchronized static ContextSelector<EJBClientContext> setupSelector() {
        final RemoteNamingEjbClientContextSelector selector = new RemoteNamingEjbClientContextSelector();
        ContextSelector<EJBClientContext> delegate = EJBClientContext.setSelector(selector);
        selector.delegate = delegate;
        return selector;
    }


    public void setCurrent(final CurrentEjbClientConnection connection) {
        currentContext.set(connection);
    }

    public void clearSelector() {
        currentContext.remove();
    }

    public EJBClientContext getCurrent() {
        final CurrentEjbClientConnection currentEjbClientConnection = currentContext.get();
        EJBClientContext local = null;
        if(currentEjbClientConnection != null) {
            local = getContext(currentEjbClientConnection.getConnection());
        }
        if (local == null) {
            return delegate.getCurrent();
        } else {
            return local;
        }
    }

    private EJBClientContext getContext(final Connection connection) {
        synchronized (contextByConnection) {
            EJBClientContext ret = contextByConnection.get(connection);
            if(ret == null) {
                ret = EJBClientContext.create();
                ret.registerConnection(connection);
                contextByConnection.put(connection, ret);
                connection.addCloseHandler(new CloseHandler<Connection>() {
                    @Override
                    public void handleClose(final Connection connection, final IOException e) {
                        synchronized (contextByConnection) {
                            contextByConnection.remove(connection);
                        }
                    }
                });
            }
            return ret;
        }
    }
}
