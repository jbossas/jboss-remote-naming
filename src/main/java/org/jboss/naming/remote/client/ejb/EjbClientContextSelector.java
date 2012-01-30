package org.jboss.naming.remote.client.ejb;

import org.jboss.ejb.client.ContextSelector;
import org.jboss.ejb.client.EJBClientContext;
import org.jboss.remoting3.Connection;

/**
 * @author John Bailey
 */
public class EjbClientContextSelector implements ContextSelector<EJBClientContext> {
    private final EJBClientContext ejbClientContext;

    public static ContextSelector<EJBClientContext> setupSelector(final Connection connection) {
        return EJBClientContext.setSelector(new EjbClientContextSelector(connection));
    }

    public static void resetSelector(final ContextSelector<EJBClientContext> selector) {
        EJBClientContext.setSelector(selector);
    }

    private EjbClientContextSelector(final Connection connection) {
        this.ejbClientContext = EJBClientContext.create();

        this.ejbClientContext.registerConnection(connection);
    }

    public EJBClientContext getCurrent() {
        return ejbClientContext;
    }
}
