package org.jboss.naming.remote.client.ejb;

import org.jboss.ejb.client.ContextSelector;
import org.jboss.ejb.client.EJBClientContext;
import org.jboss.remoting3.Connection;

/**
 * @author John Bailey
 */
public class RemoteNamingEjbClientContextSelector implements ContextSelector<EJBClientContext> {

    private ContextSelector<EJBClientContext> delegate;
    private final ThreadLocal<EJBClientContext> currentContext = new ThreadLocal<EJBClientContext>();


    public synchronized static ContextSelector<EJBClientContext> setupSelector() {
        final RemoteNamingEjbClientContextSelector selector = new RemoteNamingEjbClientContextSelector();
        ContextSelector<EJBClientContext> delegate = EJBClientContext.setSelector(selector);
        selector.delegate = delegate;
        return selector;
    }

    public void setCurrent(final Connection connection) {
        final EJBClientContext context = EJBClientContext.create();
        context.registerConnection(connection);
        currentContext.set(context);
    }

    public void clearSelector() {
        currentContext.remove();
    }

    public EJBClientContext getCurrent() {
        EJBClientContext local = currentContext.get();
        if(local == null) {
            return delegate.getCurrent();
        } else {
            return local;
        }
    }
}
