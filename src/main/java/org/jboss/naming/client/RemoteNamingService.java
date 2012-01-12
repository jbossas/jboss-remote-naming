/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.naming.client;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import javax.naming.Context;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.SaslException;
import org.jboss.logging.Logger;
import static org.jboss.naming.client.Constants.NAMING;
import org.jboss.naming.client.protocol.CancellableDataOutputStream;
import org.jboss.naming.client.protocol.Versions;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.OpenListener;
import org.jboss.remoting3.Registration;
import org.jboss.remoting3.Remoting;
import org.jboss.remoting3.remote.RemoteConnectionProviderFactory;
import org.jboss.remoting3.security.ServerAuthenticationProvider;
import org.jboss.remoting3.spi.NetworkServerProvider;
import org.jboss.sasl.callback.VerifyPasswordCallback;
import org.xnio.IoUtils;
import org.xnio.OptionMap;
import static org.xnio.Options.SASL_MECHANISMS;
import static org.xnio.Options.SASL_POLICY_NOANONYMOUS;
import static org.xnio.Options.SASL_POLICY_NOPLAINTEXT;
import static org.xnio.Options.SASL_PROPERTIES;
import static org.xnio.Options.SSL_ENABLED;
import org.xnio.Property;
import org.xnio.Sequence;
import org.xnio.Xnio;
import org.xnio.channels.AcceptingChannel;
import org.xnio.channels.ConnectedStreamChannel;

/**
 * @author John Bailey
 */
public class RemoteNamingService {
    private static final Logger log = Logger.getLogger(RemoteNamingService.class);

    public static final String ANONYMOUS = "ANONYMOUS";
    public static final String DIGEST_MD5 = "DIGEST-MD5";
    public static final String JBOSS_LOCAL_USER = "JBOSS-LOCAL-USER";
    public static final String PLAIN = "PLAIN";
    static final String REALM_PROPERTY = "com.sun.security.sasl.digest.realm";
    static final String PRE_DIGESTED_PROPERTY = "org.jboss.sasl.digest.pre_digested";
    static final String LOCAL_DEFAULT_USER = "jboss.sasl.local-user.default-user";
    static final String LOCAL_USER_CHALLENGE_PATH = "jboss.sasl.local-user.challenge-path";
    private static final String DOLLAR_LOCAL = "$local";
    private static final String REALM = "Naming_Test_Realm";

    private Endpoint endpoint;
    private Registration registration;
    private AcceptingChannel<? extends ConnectedStreamChannel> server;
    private final ServerAuthenticationProvider authenticationProvider;
    private final String host;
    private final int listenerPort;
    private final Set<String> saslMechanisms;
    private final Context localContext;
    
    private final Executor executor;

    public RemoteNamingService(final Context localContext, final String host, final int listenerPort, final ServerAuthenticationProvider authenticationProvider, final Set<String> saslMechanisms, final Executor executor) {
        this.localContext = localContext;
        this.host = host;
        this.listenerPort = listenerPort;
        this.authenticationProvider = authenticationProvider != null ? authenticationProvider : new DefaultAuthenticationHandler();
        this.saslMechanisms = saslMechanisms;
        this.executor = executor;
    } 
    
    public void start() throws IOException {
        final Xnio xnio = Xnio.getInstance();
        endpoint = Remoting.createEndpoint("RemoteNaming", xnio, OptionMap.EMPTY);
        endpoint.addConnectionProvider("remote", new RemoteConnectionProviderFactory(), OptionMap.EMPTY);

        final NetworkServerProvider nsp = endpoint.getConnectionProviderInterface("remote", NetworkServerProvider.class);
        final SocketAddress bindAddress = new InetSocketAddress("localhost", listenerPort);
        final OptionMap serverOptions = createOptionMap();

        server = nsp.createServer(bindAddress, serverOptions, authenticationProvider, null);

        registration = endpoint.registerService(Constants.CHANNEL_NAME, new ChannelOpenListener(), OptionMap.EMPTY);
    }

    private class ChannelOpenListener implements OpenListener {

        public void channelOpened(Channel channel) {
            log.trace("Channel Opened");

            // Add a close handler so we can ensure we clean up when clients disconnect.
            channel.addCloseHandler(new ChannelCloseHandler());
            try {
                writeHeader(channel);
                channel.receiveMessage(new ClientVersionReceiver());
            } catch (IOException e) {
                log.error("Unable to send header, closing channel", e);
                IoUtils.safeClose(channel);
            }

        }

        public void registrationTerminated() {
            // To change body of implemented methods use File | Settings | File Templates.
        }
    }

    private void writeHeader(final Channel channel) throws IOException {
        CancellableDataOutputStream dos = new CancellableDataOutputStream(channel.writeMessage());
        try {
            dos.writeBytes("naming");
            byte[] versions = Versions.getSupportedVersions();
            dos.write(versions.length);
            dos.write(versions);
        } catch (IOException e) {
            dos.cancel();
            throw e;
        } finally {
            dos.close();
        }
    }


    private class ClientVersionReceiver implements Channel.Receiver {
        public void handleMessage(Channel channel, MessageInputStream messageInputStream) {
            DataInputStream dis = new DataInputStream(messageInputStream);
            try {
                log.tracef("Bytes Available %d", dis.available());
                byte[] namingHeader = new byte[6];
                dis.read(namingHeader);
                log.tracef("First Three %s", new String(namingHeader));
                if (!Arrays.equals(namingHeader, NAMING)) {
                    throw new IOException("Invalid leading bytes in header.");
                }
                log.tracef("Bytes Available %d", dis.available());
                byte version = dis.readByte();
                log.debugf("Chosen version 0x0%d", version);

                Versions.getRemoteNamingServer(version, channel, RemoteNamingService.this);
            } catch (IOException e) {
                log.error("Error determining version selected by client.");
            } finally {
                IoUtils.safeClose(dis);
            }
        }

        public void handleError(final Channel channel, final IOException e) {
        }

        public void handleEnd(Channel channel) {
        }

    }
    
    private class ChannelCloseHandler implements CloseHandler<Channel> {

        public void handleClose(Channel channel, IOException e) {
            log.debug("Server handleClose");
            // TODO - Perform Clean Up - possibly notification registrations and even connection registrations.
        }
    }

    private OptionMap createOptionMap() {
        List<String> mechanisms = new LinkedList<String>();
        Set<Property> properties = new HashSet<Property>();
        OptionMap.Builder builder = OptionMap.builder();

        if (saslMechanisms.contains(JBOSS_LOCAL_USER)) {
            mechanisms.add(JBOSS_LOCAL_USER);
            builder.set(SASL_POLICY_NOPLAINTEXT, false);
            properties.add(Property.of(LOCAL_DEFAULT_USER, DOLLAR_LOCAL));
        }

        if (saslMechanisms.contains(DIGEST_MD5)) {
            mechanisms.add(DIGEST_MD5);
            properties.add(Property.of(REALM_PROPERTY, REALM));

        }

        if (saslMechanisms.contains(PLAIN)) {
            mechanisms.add(PLAIN);
            builder.set(SASL_POLICY_NOPLAINTEXT, false);
        }

        if (saslMechanisms.isEmpty() || saslMechanisms.contains(ANONYMOUS)) {
            mechanisms.add(ANONYMOUS);
            builder.set(SASL_POLICY_NOANONYMOUS, false);
        }

        // TODO - SSL Options will be added in a subsequent task.
        builder.set(SSL_ENABLED, false);

        builder.set(SASL_MECHANISMS, Sequence.of(mechanisms));
        builder.set(SASL_PROPERTIES, Sequence.of(properties));

        return builder.getMap();
    }

    public void stop() throws IOException {
        if (server != null) {
            server.close();
        }

        if (endpoint != null) {
            endpoint.close();
        }

    }

    public Context getLocalContext() {
        return localContext;
    }

    public Executor getExecutor() {
        return executor;
    }

    private class DefaultAuthenticationHandler implements ServerAuthenticationProvider {
        @Override
        public CallbackHandler getCallbackHandler(String mechanismName) {
            if (mechanismName.equals(ANONYMOUS)) {
                return new CallbackHandler() {

                    @Override
                    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                        for (Callback current : callbacks) {
                            throw new UnsupportedCallbackException(current, "ANONYMOUS mechanism so not expecting a callback");
                        }
                    }
                };

            }

            if (mechanismName.equals(DIGEST_MD5) || mechanismName.equals(PLAIN)) {
                return new CallbackHandler() {

                    @Override
                    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                        for (Callback current : callbacks) {
                            if (current instanceof NameCallback) {
                                NameCallback ncb = (NameCallback) current;
                                if (ncb.getDefaultName().equals("DigestUser") == false) {
                                    throw new IOException("Bad User");
                                }
                            } else if (current instanceof PasswordCallback) {
                                PasswordCallback pcb = (PasswordCallback) current;
                                pcb.setPassword("DigestPassword".toCharArray());
                            } else if (current instanceof VerifyPasswordCallback) {
                                VerifyPasswordCallback vpc = (VerifyPasswordCallback) current;
                                vpc.setVerified("DigestPassword".equals(vpc.getPassword()));
                            } else if (current instanceof AuthorizeCallback) {
                                AuthorizeCallback acb = (AuthorizeCallback) current;
                                acb.setAuthorized(acb.getAuthenticationID().equals(acb.getAuthorizationID()));
                            } else if (current instanceof RealmCallback) {
                                RealmCallback rcb = (RealmCallback) current;
                                if (rcb.getDefaultText().equals(REALM) == false) {
                                    throw new IOException("Bad realm");
                                }
                            } else {
                                throw new UnsupportedCallbackException(current);
                            }
                        }

                    }
                };

            }

            if (mechanismName.equals(JBOSS_LOCAL_USER)) {
                return new CallbackHandler() {

                    @Override
                    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                        for (Callback current : callbacks) {
                            if (current instanceof NameCallback) {
                                NameCallback ncb = (NameCallback) current;
                                if (DOLLAR_LOCAL.equals(ncb.getDefaultName()) == false) {
                                    throw new SaslException("Only " + DOLLAR_LOCAL + " user is acceptable.");
                                }
                            } else if (current instanceof AuthorizeCallback) {
                                AuthorizeCallback acb = (AuthorizeCallback) current;
                                acb.setAuthorized(acb.getAuthenticationID().equals(acb.getAuthorizationID()));
                            } else {
                                throw new UnsupportedCallbackException(current);
                            }
                        }

                    }
                };

            }

            return null;
        }

    }
}

