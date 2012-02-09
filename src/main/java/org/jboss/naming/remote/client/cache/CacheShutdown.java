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
    private final ConnectionCache connectionCache;


    public CacheShutdown(final ConnectionCache connectionCache, final EndpointCache endpointCache) {
        this.connectionCache = connectionCache;
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
        connectionCache.shutdown();
        endpointCache.shutdown();
    }
}
