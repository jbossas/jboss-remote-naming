package org.jboss.naming.remote.client.cache;

import java.io.IOException;
import java.net.URI;
import java.security.Principal;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.callback.CallbackHandler;

import org.jboss.naming.remote.protocol.IoFutureHelper;
import org.jboss.remoting3.Attachments;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3.Endpoint;
import org.xnio.IoFuture;
import org.xnio.OptionMap;

/**
 * @author John Bailey
 */
public class ConnectionCache {
    private final ConcurrentMap<CacheKey, CacheEntry> cache = new ConcurrentHashMap<CacheKey, CacheEntry>();

    public synchronized Connection get(final Endpoint clientEndpoint, final URI connectionURI, final OptionMap connectOptions, final CallbackHandler callbackHandler, final long connectionTimeout) throws IOException {
        final CacheKey key = new CacheKey(callbackHandler.getClass(), connectOptions, connectionURI);
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
        private volatile Connection connection;

        private CacheEntry(Connection connection) {
            this.connection = connection;
        }
    }

    private static final class CacheKey {
        final URI destination;
        final OptionMap connectOptions;
        final Class<?> callbackHandlerClass;

        private CacheKey(final Class<?> callbackHandlerClass, final OptionMap connectOptions, final URI destination) {
            this.callbackHandlerClass = callbackHandlerClass;
            this.connectOptions = connectOptions;
            this.destination = destination;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final CacheKey cacheKey = (CacheKey) o;

            if (callbackHandlerClass != null ? !callbackHandlerClass.equals(cacheKey.callbackHandlerClass) : cacheKey.callbackHandlerClass != null)
                return false;
            if (connectOptions != null ? !connectOptions.equals(cacheKey.connectOptions) : cacheKey.connectOptions != null)
                return false;
            if (destination != null ? !destination.equals(cacheKey.destination) : cacheKey.destination != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = destination != null ? destination.hashCode() : 0;
            result = 31 * result + (connectOptions != null ? connectOptions.hashCode() : 0);
            result = 31 * result + (callbackHandlerClass != null ? callbackHandlerClass.hashCode() : 0);
            return result;
        }
    }
}
