package org.jboss.naming.remote.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.naming.NamingException;
import javax.security.auth.callback.CallbackHandler;

import org.jboss.logging.Logger;
import org.jboss.naming.remote.protocol.IoFutureHelper;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3.Endpoint;
import org.xnio.IoFuture;
import org.xnio.OptionMap;

/**
 * @author John Bailey
 * @author Stuart Douglas
 */
public class NamingStoreCache {
    private static final Logger logger = Logger.getLogger(NamingStoreCache.class);

    private final ConcurrentMap<CacheKey, CacheEntry> cache = new ConcurrentHashMap<CacheKey, CacheEntry>();

    /**
     * Returns a {@link Channel} for the passed connection properties. If the connection is already created
     * and cached for the passed connection properties, then the cached channel will be returned. Else a new
     * connection and channel will be created and that new channel returned.
     *
     * @param clientEndpoint                 The {@link org.jboss.remoting3.Endpoint} that will be used to open a connection
     * @param connectionURI                  The connection URI
     * @param connectOptions                 The options to be used for connection creation
     * @param callbackHandler                The callback handler to be used for connection creation
     * @param connectionTimeout              The connection timeout in milli seconds that will be used while creating a connection
     * @param channelCreationOptions         The {@link org.xnio.OptionMap options} that will be used if/when the channel is created
     * @param channelCreationTimeoutInMillis The timeout in milli seconds, that will be used while opening a channel
     * @param contextCloseTasks              The tasks to be performed when the context is closed
     * @return
     * @throws IOException
     */
    public synchronized RemoteNamingStore getRemoteNamingStore(final Endpoint clientEndpoint, final URI connectionURI, final OptionMap connectOptions, final CallbackHandler callbackHandler, final long connectionTimeout,
                                                               final OptionMap channelCreationOptions, final long channelCreationTimeoutInMillis, final Hashtable<String, Object> env, final List<RemoteContext.CloseTask> contextCloseTasks) throws IOException, NamingException {
        final CacheKey key = new CacheKey(clientEndpoint, callbackHandler.getClass(), connectOptions, connectionURI);
        CacheEntry cacheEntry = cache.get(key);
        if (cacheEntry == null) {
            final IoFuture<Connection> futureConnection = clientEndpoint.connect(connectionURI, connectOptions, callbackHandler);
            final Connection connection = IoFutureHelper.get(futureConnection, connectionTimeout, TimeUnit.MILLISECONDS);
            // open a channel
            final IoFuture<Channel> futureChannel = connection.openChannel("naming", channelCreationOptions);
            final Channel channel = IoFutureHelper.get(futureChannel, channelCreationTimeoutInMillis, TimeUnit.MILLISECONDS);
            final RemoteNamingStore store = RemoteContextFactory.createVersionedStore(channel);
            cacheEntry = new CacheEntry(connection, store);
            cache.put(key, cacheEntry);
        }

        //when the context is closed we need to release and decrease the reference count
        contextCloseTasks.add(new RemoteContext.CloseTask() {
            @Override
            public void close(final boolean isFinalize) {
                release(key, isFinalize);
            }
        });
        cacheEntry.referenceCount.incrementAndGet();
        return cacheEntry.namingStore;
    }

    public synchronized void release(final CacheKey connectionHash, final boolean async) {
        final CacheEntry cacheEntry = cache.get(connectionHash);
        if (cacheEntry.referenceCount.decrementAndGet() == 0) {
            try {
                if (async) {
                    cacheEntry.connection.closeAsync();
                } else {
                    try {
                        cacheEntry.connection.close();
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to close connection", e);
                    }
                }

            } finally {
                cache.remove(connectionHash);
            }
        }
    }

    public synchronized void shutdown() {
        for (Map.Entry<CacheKey, CacheEntry> entry : cache.entrySet()) {
            safeClose(entry.getValue().connection);
        }
    }

    private class CacheEntry {
        private final AtomicInteger referenceCount = new AtomicInteger(0);
        private final Connection connection;
        private final RemoteNamingStore namingStore;

        private CacheEntry(final Connection connection, final RemoteNamingStore namingStore) {
            this.connection = connection;
            this.namingStore = namingStore;
        }
    }

    private static final class CacheKey {
        final Endpoint endpoint;
        final URI destination;
        final OptionMap connectOptions;
        final Class<?> callbackHandlerClass;

        private CacheKey(final Endpoint endpoint, final Class<?> callbackHandlerClass, final OptionMap connectOptions, final URI destination) {
            this.endpoint = endpoint;
            this.callbackHandlerClass = callbackHandlerClass;
            this.connectOptions = connectOptions;
            this.destination = destination;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CacheKey cacheKey = (CacheKey) o;

            if (callbackHandlerClass != null ? !callbackHandlerClass.equals(cacheKey.callbackHandlerClass) : cacheKey.callbackHandlerClass != null)
                return false;
            if (connectOptions != null ? !connectOptions.equals(cacheKey.connectOptions) : cacheKey.connectOptions != null)
                return false;
            if (destination != null ? !destination.equals(cacheKey.destination) : cacheKey.destination != null)
                return false;
            if (endpoint != null ? !endpoint.equals(cacheKey.endpoint) : cacheKey.endpoint != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = endpoint != null ? endpoint.hashCode() : 0;
            result = 31 * result + (destination != null ? destination.hashCode() : 0);
            result = 31 * result + (connectOptions != null ? connectOptions.hashCode() : 0);
            result = 31 * result + (callbackHandlerClass != null ? callbackHandlerClass.hashCode() : 0);
            return result;
        }
    }


    private static void safeClose(Closeable closable) {
        try {
            closable.close();
        } catch (Throwable t) {
            logger.debug("Failed to close connection ", t);
        }
    }
}
