package org.jboss.naming.remote.client.cache;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Class which registers a shutdown hook to close any outstanding endpoints and connections.
 *
 * @author Stuart Douglas
 */
public class CacheShutdown implements Runnable {

    private final EndpointCache endpointCache;
    private final ContextCache contextCache;


    public CacheShutdown(final ContextCache contextCache, final EndpointCache endpointCache) {
        this.contextCache = contextCache;
        this.endpointCache = endpointCache;
    }

    public void registerShutdownHandler() {
        AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                Runtime.getRuntime().addShutdownHook(new Thread(CacheShutdown.this));
                return null;
            }
        });
    }


    public void run() {
        contextCache.shutdown();
        endpointCache.shutdown();
    }
}
