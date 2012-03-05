package org.jboss.naming.remote.client.cache;

import org.jboss.logging.Logger;
import org.jboss.naming.remote.protocol.IoFutureHelper;
import org.jboss.remoting3.Attachments;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3.Endpoint;
import org.xnio.IoFuture;
import org.xnio.OptionMap;

import javax.security.auth.callback.CallbackHandler;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.security.Principal;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author John Bailey
 */
public class ConnectionCache {
    private static final Logger logger = Logger.getLogger(ConnectionCache.class);

    private final ConcurrentMap<CacheKey, CacheEntry> cache = new ConcurrentHashMap<CacheKey, CacheEntry>();

    /**
     * @param clientEndpoint
     * @param connectionURI
     * @param connectOptions
     * @param callbackHandler
     * @param connectionTimeout
     * @return
     * @throws IOException
     * @deprecated Since 1.0.2.Final. Use {@link #getChannel(org.jboss.remoting3.Endpoint, java.net.URI, org.xnio.OptionMap, javax.security.auth.callback.CallbackHandler, long, org.xnio.OptionMap, long)}
     *             instead
     */
    @Deprecated
    public synchronized Connection get(final Endpoint clientEndpoint, final URI connectionURI, final OptionMap connectOptions, final CallbackHandler callbackHandler, final long connectionTimeout) throws IOException {
        final CacheKey key = new CacheKey(clientEndpoint, callbackHandler.getClass(), connectOptions, connectionURI);
        CacheEntry cacheEntry = cache.get(key);
        if (cacheEntry == null) {
            synchronized (this) {
                cacheEntry = cache.get(key);
                if (cacheEntry == null) {
                    final IoFuture<Connection> futureConnection = clientEndpoint.connect(connectionURI, connectOptions, callbackHandler);
                    cacheEntry = new CacheEntry(new ConnectionWrapper(key, IoFutureHelper.get(futureConnection, connectionTimeout, TimeUnit.MILLISECONDS)));
                    cache.put(key, cacheEntry);
                }
            }
        }
        cacheEntry.referenceCount.incrementAndGet();
        return cacheEntry.connection;
    }

    /**
     * Returns a {@link Channel} for the passed connection properties. If the connection is already created
     * and cached for the passed connection properties, then the cached channel will be returned. Else a new
     * connection and channel will be created and that new channel returned.
     *
     * @param clientEndpoint                 The {@link Endpoint} that will be used to open a connection
     * @param connectionURI                  The connection URI
     * @param connectOptions                 The options to be used for connection creation
     * @param callbackHandler                The callback handler to be used for connection creation
     * @param connectionTimeout              The connection timeout in milli seconds that will be used while creating a connection
     * @param channelCreationOptions         The {@link OptionMap options} that will be used if/when the channel is created
     * @param channelCreationTimeoutInMillis The timeout in milli seconds, that will be used while opening a channel
     * @return
     * @throws IOException
     */
    public synchronized Channel getChannel(final Endpoint clientEndpoint, final URI connectionURI, final OptionMap connectOptions, final CallbackHandler callbackHandler, final long connectionTimeout,
                                           final OptionMap channelCreationOptions, final long channelCreationTimeoutInMillis) throws IOException {
        final CacheKey key = new CacheKey(clientEndpoint, callbackHandler.getClass(), connectOptions, connectionURI);
        CacheEntry cacheEntry = cache.get(key);
        if (cacheEntry == null) {
            synchronized (this) {
                cacheEntry = cache.get(key);
                if (cacheEntry == null) {
                    final IoFuture<Connection> futureConnection = clientEndpoint.connect(connectionURI, connectOptions, callbackHandler);
                    final Connection connection = IoFutureHelper.get(futureConnection, connectionTimeout, TimeUnit.MILLISECONDS);
                    // open a channel
                    final IoFuture<Channel> futureChannel = connection.openChannel("naming", channelCreationOptions);
                    final Channel channel = IoFutureHelper.get(futureChannel, channelCreationTimeoutInMillis, TimeUnit.MILLISECONDS);
                    cacheEntry = new CacheEntry(new ConnectionWrapper(key, connection), channel);
                    cache.put(key, cacheEntry);
                }
            }
        }
        cacheEntry.referenceCount.incrementAndGet();
        return cacheEntry.channel;
    }

    public void release(final Object connectionHash) {
        this.release(connectionHash, false);
    }

    public synchronized void release(final Object connectionHash, final boolean async) {
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

    private class ConnectionWrapper implements Connection {
        private final CacheKey connectionHash;
        private final Connection delegate;

        private ConnectionWrapper(final CacheKey connectionHash, final Connection delegate) {
            this.delegate = delegate;
            this.connectionHash = connectionHash;
        }

        public Collection<Principal> getPrincipals() {
            return delegate.getPrincipals();
        }

        public IoFuture<Channel> openChannel(final String s, final OptionMap optionMap) {
            return delegate.openChannel(s, optionMap);
        }

        public String getRemoteEndpointName() {
            return delegate.getRemoteEndpointName();
        }

        public Endpoint getEndpoint() {
            return delegate.getEndpoint();
        }

        public void close() throws IOException {
            ConnectionCache.this.release(connectionHash);
        }

        public void awaitClosed() throws InterruptedException {
            delegate.awaitClosed();
        }

        public void awaitClosedUninterruptibly() {
            delegate.awaitClosedUninterruptibly();
        }

        public void closeAsync() {
            ConnectionCache.this.release(connectionHash, true);
        }

        public Key addCloseHandler(CloseHandler<? super Connection> closeHandler) {
            return delegate.addCloseHandler(closeHandler);
        }

        public Attachments getAttachments() {
            return delegate.getAttachments();
        }
    }

    private class CacheEntry {
        private final AtomicInteger referenceCount = new AtomicInteger(0);
        private final Connection connection;
        private final Channel channel;

        private CacheEntry(final Connection connection) {
            this.connection = connection;
            this.channel = null;
        }

        private CacheEntry(final Connection connection, final Channel channel) {
            this.connection = connection;
            this.channel = channel;
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
