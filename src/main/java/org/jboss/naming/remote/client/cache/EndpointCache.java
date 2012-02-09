package org.jboss.naming.remote.client.cache;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.security.auth.callback.CallbackHandler;
import org.jboss.remoting3.Attachments;
import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3.DuplicateRegistrationException;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.OpenListener;
import org.jboss.remoting3.Registration;
import org.jboss.remoting3.Remoting;
import org.jboss.remoting3.ServiceRegistrationException;
import org.jboss.remoting3.UnknownURISchemeException;
import org.jboss.remoting3.remote.RemoteConnectionProviderFactory;
import org.jboss.remoting3.spi.ConnectionProviderFactory;
import org.xnio.IoFuture;
import org.xnio.OptionMap;
import org.xnio.XnioWorker;
import org.xnio.ssl.XnioSsl;

/**
 * @author John Bailey
 */
public class EndpointCache {
    private final ConcurrentMap<CacheKey, CacheEntry> cache = new ConcurrentHashMap<CacheKey, CacheEntry>();

    public synchronized Endpoint get(final String endpointName, final OptionMap endPointCreationOptions, final OptionMap remoteConnectionProviderOptions) throws IOException {
        final CacheKey endpointHash = new CacheKey(remoteConnectionProviderOptions, endPointCreationOptions, endpointName);
        CacheEntry cacheEntry = cache.get(endpointHash);
        if (cacheEntry == null) {
            final Endpoint endpoint = Remoting.createEndpoint(endpointName, endPointCreationOptions);
            endpoint.addConnectionProvider("remote", new RemoteConnectionProviderFactory(), remoteConnectionProviderOptions);
            cacheEntry = new CacheEntry(new EndpointWrapper(endpointHash, endpoint));

            cache.putIfAbsent(endpointHash, cacheEntry);
        }
        cacheEntry.referenceCount.incrementAndGet();
        return cacheEntry.endpoint;
    }

    public void release(final CacheKey connectionHash) {
        this.release(connectionHash, false);
    }

    public synchronized void release(final CacheKey endpointHash, final boolean async) {
        final CacheEntry cacheEntry = cache.get(endpointHash);
        if (cacheEntry.referenceCount.decrementAndGet() == 0) {
            try {
                if (async) {
                    cacheEntry.endpoint.closeAsync();
                } else {
                    try {
                        cacheEntry.endpoint.close();
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to close endpoint", e);
                    }
                }

            } finally {
                cache.remove(endpointHash);
            }
        }
    }

    private class EndpointWrapper implements Endpoint {
        private final CacheKey endpointHash;
        private final Endpoint endpoint;

        private EndpointWrapper(final CacheKey endpointHash, final Endpoint endpoint) {
            this.endpointHash = endpointHash;
            this.endpoint = endpoint;
        }

        public String getName() {
            return endpoint.getName();
        }

        public Registration registerService(String s, OpenListener openListener, OptionMap optionMap) throws ServiceRegistrationException {
            return endpoint.registerService(s, openListener, optionMap);
        }

        public IoFuture<Connection> connect(URI uri) throws IOException {
            return endpoint.connect(uri);
        }

        public IoFuture<Connection> connect(URI uri, OptionMap optionMap) throws IOException {
            return endpoint.connect(uri, optionMap);
        }

        public IoFuture<Connection> connect(URI uri, OptionMap optionMap, CallbackHandler callbackHandler) throws IOException {
            return endpoint.connect(uri, optionMap, callbackHandler);
        }

        public IoFuture<Connection> connect(URI uri, OptionMap optionMap, CallbackHandler callbackHandler, SSLContext sslContext) throws IOException {
            return endpoint.connect(uri, optionMap, callbackHandler, sslContext);
        }

        public IoFuture<Connection> connect(URI uri, OptionMap optionMap, CallbackHandler callbackHandler, XnioSsl xnioSsl) throws IOException {
            return endpoint.connect(uri, optionMap, callbackHandler, xnioSsl);
        }

        public IoFuture<Connection> connect(URI uri, OptionMap optionMap, String s, String s1, char[] chars) throws IOException {
            return endpoint.connect(uri, optionMap, s, s1, chars);
        }

        public IoFuture<Connection> connect(URI uri, OptionMap optionMap, String s, String s1, char[] chars, SSLContext sslContext) throws IOException {
            return endpoint.connect(uri, optionMap, s, s1, chars, sslContext);
        }

        public IoFuture<Connection> connect(URI uri, OptionMap optionMap, String s, String s1, char[] chars, XnioSsl xnioSsl) throws IOException {
            return endpoint.connect(uri, optionMap, s, s1, chars, xnioSsl);
        }

        public IoFuture<Connection> connect(String s, SocketAddress socketAddress, SocketAddress socketAddress1) throws IOException {
            return endpoint.connect(s, socketAddress, socketAddress1);
        }

        public IoFuture<Connection> connect(String s, SocketAddress socketAddress, SocketAddress socketAddress1, OptionMap optionMap) throws IOException {
            return endpoint.connect(s, socketAddress, socketAddress1, optionMap);
        }

        public IoFuture<Connection> connect(String s, SocketAddress socketAddress, SocketAddress socketAddress1, OptionMap optionMap, CallbackHandler callbackHandler) throws IOException {
            return endpoint.connect(s, socketAddress, socketAddress1, optionMap, callbackHandler);
        }

        public IoFuture<Connection> connect(String s, SocketAddress socketAddress, SocketAddress socketAddress1, OptionMap optionMap, CallbackHandler callbackHandler, SSLContext sslContext) throws IOException {
            return endpoint.connect(s, socketAddress, socketAddress1, optionMap, callbackHandler, sslContext);
        }

        public IoFuture<Connection> connect(String s, SocketAddress socketAddress, SocketAddress socketAddress1, OptionMap optionMap, CallbackHandler callbackHandler, XnioSsl xnioSsl) throws IOException {
            return endpoint.connect(s, socketAddress, socketAddress1, optionMap, callbackHandler, xnioSsl);
        }

        public IoFuture<Connection> connect(String s, SocketAddress socketAddress, SocketAddress socketAddress1, OptionMap optionMap, String s1, String s2, char[] chars) throws IOException {
            return endpoint.connect(s, socketAddress, socketAddress1, optionMap, s1, s2, chars);
        }

        public IoFuture<Connection> connect(String s, SocketAddress socketAddress, SocketAddress socketAddress1, OptionMap optionMap, String s1, String s2, char[] chars, SSLContext sslContext) throws IOException {
            return endpoint.connect(s, socketAddress, socketAddress1, optionMap, s1, s2, chars, sslContext);
        }

        public IoFuture<Connection> connect(String s, SocketAddress socketAddress, SocketAddress socketAddress1, OptionMap optionMap, String s1, String s2, char[] chars, XnioSsl xnioSsl) throws IOException {
            return endpoint.connect(s, socketAddress, socketAddress1, optionMap, s1, s2, chars, xnioSsl);
        }

        public Registration addConnectionProvider(String s, ConnectionProviderFactory connectionProviderFactory, OptionMap optionMap) throws DuplicateRegistrationException, IOException {
            return endpoint.addConnectionProvider(s, connectionProviderFactory, optionMap);
        }

        public <T> T getConnectionProviderInterface(String s, Class<T> tClass) throws UnknownURISchemeException, ClassCastException {
            return endpoint.getConnectionProviderInterface(s, tClass);
        }

        public boolean isValidUriScheme(String s) {
            return endpoint.isValidUriScheme(s);
        }

        public XnioWorker getXnioWorker() {
            return endpoint.getXnioWorker();
        }

        public void close() throws IOException {
            EndpointCache.this.release(endpointHash);
        }

        public void awaitClosed() throws InterruptedException {
            endpoint.awaitClosed();
        }

        public void awaitClosedUninterruptibly() {
            endpoint.awaitClosedUninterruptibly();
        }

        public void closeAsync() {
            EndpointCache.this.release(endpointHash, true);
        }

        public Key addCloseHandler(CloseHandler<? super Endpoint> closeHandler) {
            return endpoint.addCloseHandler(closeHandler);
        }

        public Attachments getAttachments() {
            return endpoint.getAttachments();
        }
    }

    private static  class CacheEntry {
        private final AtomicInteger referenceCount = new AtomicInteger(0);
        private volatile Endpoint endpoint;

        private CacheEntry(final Endpoint endpoint) {
            this.endpoint = endpoint;
        }
    }

    private static class CacheKey {
        final String endpointName;
        final OptionMap connectOptions;
        final OptionMap remoteConnectionProviderOptions;

        private CacheKey(final OptionMap remoteConnectionProviderOptions, final OptionMap connectOptions, final String endpointName) {
            this.remoteConnectionProviderOptions = remoteConnectionProviderOptions;
            this.connectOptions = connectOptions;
            this.endpointName = endpointName;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final CacheKey cacheKey = (CacheKey) o;

            if (connectOptions != null ? !connectOptions.equals(cacheKey.connectOptions) : cacheKey.connectOptions != null)
                return false;
            if (endpointName != null ? !endpointName.equals(cacheKey.endpointName) : cacheKey.endpointName != null)
                return false;
            if (remoteConnectionProviderOptions != null ? !remoteConnectionProviderOptions.equals(cacheKey.remoteConnectionProviderOptions) : cacheKey.remoteConnectionProviderOptions != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = endpointName != null ? endpointName.hashCode() : 0;
            result = 31 * result + (connectOptions != null ? connectOptions.hashCode() : 0);
            result = 31 * result + (remoteConnectionProviderOptions != null ? remoteConnectionProviderOptions.hashCode() : 0);
            return result;
        }
    }
}
