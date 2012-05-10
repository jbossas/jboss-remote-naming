package org.jboss.naming.remote.client;

import org.jboss.logging.Logger;
import org.jboss.naming.remote.protocol.IoFutureHelper;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3.Endpoint;
import org.xnio.IoFuture;
import org.xnio.OptionMap;

import javax.naming.NamingException;
import javax.security.auth.callback.CallbackHandler;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
     * @param connectionURL                  The connection URL
     * @param connectOptions                 The options to be used for connection creation
     * @param callbackHandler                The callback handler to be used for connection creation
     * @param connectionTimeout              The connection timeout in milli seconds that will be used while creating a connection
     * @param channelCreationOptions         The {@link org.xnio.OptionMap options} that will be used if/when the channel is created
     * @param channelCreationTimeoutInMillis The timeout in milli seconds, that will be used while opening a channel
     * @param contextCloseTasks              The tasks to be performed when the context is closed
     * @return
     * @throws IOException
     */
    public synchronized RemoteNamingStore getRemoteNamingStore(final Endpoint clientEndpoint, final String connectionURL, final OptionMap connectOptions, final CallbackHandler callbackHandler, final long connectionTimeout,
                                                               final OptionMap channelCreationOptions, final long channelCreationTimeoutInMillis, final List<RemoteContext.CloseTask> contextCloseTasks, boolean randomServer) throws IOException, NamingException, URISyntaxException {
        final CacheKey key = new CacheKey(clientEndpoint, callbackHandler.getClass(), connectOptions, connectionURL);
        CacheEntry cacheEntry = cache.get(key);
        Connection connection = null;
        if (cacheEntry == null) {
            RemoteNamingStore store;
            if (connectionURL.contains(",")) {
                //HA context
                String[] urls = connectionURL.split(",");
                List<URI> connectionUris = new ArrayList<URI>(urls.length);
                for (final String url : urls) {
                    connectionUris.add(parseRemotingURI(url));
                }
                store = new HaRemoteNamingStore(channelCreationTimeoutInMillis, channelCreationOptions, connectionTimeout, callbackHandler, connectOptions, connectionUris, clientEndpoint, randomServer);
            } else {
                final IoFuture<Connection> futureConnection = clientEndpoint.connect(parseRemotingURI(connectionURL), connectOptions, callbackHandler);
                connection = IoFutureHelper.get(futureConnection, connectionTimeout, TimeUnit.MILLISECONDS);
                // open a channel
                final IoFuture<Channel> futureChannel = connection.openChannel("naming", channelCreationOptions);
                final Channel channel = IoFutureHelper.get(futureChannel, channelCreationTimeoutInMillis, TimeUnit.MILLISECONDS);
                store = RemoteContextFactory.createVersionedStore(channel);
            }
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
                cacheEntry.namingStore.close();
            } catch (NamingException e) {
                throw new RuntimeException("Failed to close naming store", e);
            } finally {
                try {
                    if (cacheEntry.connection != null) {
                        if (async) {
                            cacheEntry.connection.closeAsync();
                        } else {
                            try {
                                cacheEntry.connection.close();
                            } catch (IOException e) {
                                throw new RuntimeException("Failed to close connection", e);
                            }
                        }
                    }
                } finally {
                    try {
                    } finally {
                        cache.remove(connectionHash);
                    }
                }
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
        final String destination;
        final OptionMap connectOptions;
        final Class<?> callbackHandlerClass;

        private CacheKey(final Endpoint endpoint, final Class<?> callbackHandlerClass, final OptionMap connectOptions, final String destination) {
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

    private static URI parseRemotingURI(final String uriString) throws URISyntaxException {
        if (!uriString.startsWith("remote://")) {
            // TODO: This project doesn't yet have i18n logging
            throw new URISyntaxException(uriString, "Unrecognized URI protocol in URI");
        }
        if (uriString.length() == "remote://".length()) {
            throw new URISyntaxException(uriString, "Unparsable URI");
        }
        final String uriWithoutProtocol = uriString.substring("remote://".length());
        final int portStart = uriWithoutProtocol.lastIndexOf(":"); // the index where the "port" starts
        if (portStart == -1 || portStart == uriWithoutProtocol.length() - 1) {
            throw new URISyntaxException(uriString, "Unparsable URI");
        }
        final String host = uriWithoutProtocol.substring(0, portStart);
        final String port = uriWithoutProtocol.substring(portStart + 1);
        return new URI("remote://" + formatPossibleIpv6Address(host) + ":" + port);
    }

    private static String formatPossibleIpv6Address(String address) {
        if (address == null) {
            return address;
        }
        if (!address.contains(":")) {
            return address;
        }
        if (address.startsWith("[") && address.endsWith("]")) {
            return address;
        }
        return "[" + address + "]";
    }
}
