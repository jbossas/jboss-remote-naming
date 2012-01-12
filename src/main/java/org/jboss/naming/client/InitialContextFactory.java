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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.xml.bind.DatatypeConverter;
import org.jboss.logging.Logger;
import static org.jboss.naming.client.ClientUtil.namingException;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.Remoting;
import org.jboss.remoting3.remote.RemoteConnectionProviderFactory;
import org.xnio.IoFuture;
import org.xnio.Option;
import org.xnio.OptionMap;
import org.xnio.Options;

/**
 * @author John Bailey
 */
public class InitialContextFactory implements javax.naming.spi.InitialContextFactory {
    private static final Logger logger = Logger.getLogger(InitialContextFactory.class);

    private static final String CLIENT_PROPS_FILE_NAME = "jboss-naming-client.properties";

    private static final long DEFAULT_CONNECTION_TIMEOUT_IN_MILLIS = 5000;
    private static final String CLIENT_PROP_KEY_ENDPOINT_NAME = "jboss.naming.client.endpoint.name";

    private static final String CLIENT_PROP_KEY_CONNECT_TIMEOUT = "jboss.naming.client.connect.timeout";
    private static final String ENDPOINT_CREATION_OPTIONS_PREFIX = "jboss.naming.client.endpoint.create.options.";
    private static final String CONNECT_OPTIONS_PREFIX = "jboss.naming.client.connect.options.";
    private static final String REMOTE_CONNECTION_PROVIDER_CREATE_OPTIONS_PREFIX = "jboss.naming.client.remote.connectionprovider.create.options.";

    private static final String CALLBACK_HANDLER_KEY = "jboss.naming.client.security.callback.handler.class";
    private static final String PASSWORD_BASE64_KEY = "jboss.naming.client.security.password.base64";
    private static final String REALM_KEY = "jboss.naming.client.security.realm";

    // The default options that will be used (unless overridden by the config file) for endpoint creation
    private static final OptionMap DEFAULT_ENDPOINT_CREATION_OPTIONS = OptionMap.create(Options.THREAD_DAEMON, true);

    // The default options that will be used (unless overridden by the config file) while creating a connection
    private static final OptionMap DEFAULT_CONNECTION_CREATION_OPTIONS = OptionMap.EMPTY;

    // The default options that will be used (unless overridden by the config file) while adding a remote connection
    // provider to the endpoint
    private static final OptionMap DEFAULT_CONNECTION_PROVIDER_CREATION_OPTIONS = OptionMap.EMPTY;

    @SuppressWarnings("unchecked")
    public Context getInitialContext(final Hashtable<?, ?> env) throws NamingException {
        final Properties clientProperties = findAndCreateClientProperties(env);
        try {
            final Connection connection = createConnection(clientProperties);
            // Now open the channel
            final IoFuture<Channel> futureChannel = connection.openChannel("naming", OptionMap.EMPTY);
            final Channel channel = IoFutureHelper.get(futureChannel, DEFAULT_CONNECTION_TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS);
            return RemoteContextFactory.createVersionedContext(channel, (Hashtable<String, Object>) env);
        } catch (NamingException e) {
            throw e;
        } catch (Throwable t) {
            throw namingException("Failed to create remoting connection", t);
        }
    }

    private Endpoint createEndpoint(final Properties clientProperties) throws IOException {
        String clientEndpointName = clientProperties.getProperty(CLIENT_PROP_KEY_ENDPOINT_NAME);
        if(clientEndpointName == null) {
            clientEndpointName = "config-based-naming-client-endpoint";
        }
        final OptionMap endPointCreationOptionsFromConfiguration = this.getOptionMapFromProperties(clientProperties, ENDPOINT_CREATION_OPTIONS_PREFIX);
        // merge with defaults
        final OptionMap endPointCreationOptions = this.mergeWithDefaults(DEFAULT_ENDPOINT_CREATION_OPTIONS, endPointCreationOptionsFromConfiguration);
        // create the endpoint
        final Endpoint clientEndpoint = Remoting.createEndpoint(clientEndpointName, endPointCreationOptions);
        // add a connection provider for the "remote" URI scheme
        final OptionMap remoteConnectionProivderOptionsFromConfiguration = this.getOptionMapFromProperties(clientProperties, REMOTE_CONNECTION_PROVIDER_CREATE_OPTIONS_PREFIX);
        // merge with defaults
        final OptionMap remoteConnectionProivderOptions = this.mergeWithDefaults(DEFAULT_CONNECTION_PROVIDER_CREATION_OPTIONS, remoteConnectionProivderOptionsFromConfiguration);
        clientEndpoint.addConnectionProvider("remote", new RemoteConnectionProviderFactory(), remoteConnectionProivderOptions);
        return clientEndpoint;
    }

    private Connection createConnection(final Properties clientProperties) throws IOException, URISyntaxException, NamingException {
        final String connectionUrl = clientProperties.getProperty(Context.PROVIDER_URL);
        if (connectionUrl == null || connectionUrl.trim().isEmpty()) {
            throw new NamingException("No provider URL configured for connection");
        }
        // get connect options for the connection

        final OptionMap connectOptionsFromConfiguration = this.getOptionMapFromProperties(clientProperties, CONNECT_OPTIONS_PREFIX);
        // merge with defaults
        final OptionMap connectOptions = this.mergeWithDefaults(DEFAULT_CONNECTION_CREATION_OPTIONS, connectOptionsFromConfiguration);
        // create the connection, but first create the endpoint
        final Endpoint clientEndpoint = createEndpoint(clientProperties);
        long connectionTimeout = DEFAULT_CONNECTION_TIMEOUT_IN_MILLIS;
        final String connectionTimeoutValue = clientProperties.getProperty(CLIENT_PROP_KEY_CONNECT_TIMEOUT);
        // if a connection timeout is specified, use it
        if (connectionTimeoutValue != null && !connectionTimeoutValue.trim().isEmpty()) {
            try {
                connectionTimeout = Long.parseLong(connectionTimeoutValue.trim());
            } catch (NumberFormatException nfe) {
                logger.info("Incorrect timeout value " + connectionTimeoutValue + " specified. Falling back to default connection timeout value " + DEFAULT_CONNECTION_TIMEOUT_IN_MILLIS + " milli secondss");
            }
        }
        final CallbackHandler callbackHandler = createCallbackHandler(clientProperties);

        final URI connectionURI = new URI(connectionUrl);
        // TODO: FIXME: The AnonymousCallbackHandler being passed here is a hack, till we have
        // a better way of configuring security via EJB client configuration file
        final IoFuture<Connection> futureConnection = clientEndpoint.connect(connectionURI, connectOptions, callbackHandler);
        // wait for the connection to be established
        return IoFutureHelper.get(futureConnection, connectionTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a callback handler for the given remote connection.
     *
     * @param clientProperties The connection properties  @return The CallbackHandler.
     */
    private CallbackHandler createCallbackHandler(final Properties clientProperties) throws NamingException {
        String callbackClass = clientProperties.getProperty(CALLBACK_HANDLER_KEY);
        String userName = clientProperties.getProperty(Context.SECURITY_PRINCIPAL);
        String password = clientProperties.getProperty(Context.SECURITY_CREDENTIALS);
        String passwordBase64 = clientProperties.getProperty(PASSWORD_BASE64_KEY);
        String realm = clientProperties.getProperty(REALM_KEY);

        CallbackHandler handler = resolveCallbackHandler(callbackClass, userName, password, passwordBase64, realm);
        if (handler != null) {
            return handler;
        }

        //no auth specified, just use the default
        return new AnonymousCallbackHandler();
    }

    private CallbackHandler resolveCallbackHandler(final String callbackClass, final String userName, final String password, final String passwordBase64, final String realm) throws NamingException {

        if (callbackClass != null && (userName != null || password != null)) {
            throw new RuntimeException("Cannot specify both a callback handler and a username/password for connection.");
        }
        if (callbackClass != null) {
            ClassLoader classLoader = getClientClassLoader();
            try {
                final Class<?> clazz = Class.forName(callbackClass, true, classLoader);
                return (CallbackHandler) clazz.newInstance();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Could not load callback handler class " + callbackClass, e);
            } catch (Exception e) {
                throw namingException("Could not instantiate handler instance of type " + callbackClass, e);
            }
        } else if (userName != null) {
            if (password != null && passwordBase64 != null) {
                throw new NamingException("Cannot specify both a plain text and base64 encoded password");
            }

            final String decodedPassword;
            if (passwordBase64 != null) {
                try {
                    decodedPassword = DatatypeConverter.printBase64Binary(passwordBase64.getBytes());
                } catch (Exception e) {
                    throw namingException("Could not decode base64 encoded password for connection", e);
                }
            } else if (password != null) {
                decodedPassword = password;
            } else {
                decodedPassword = null;
            }
            return new AuthenticationCallbackHandler(userName, decodedPassword.toCharArray(), realm);
        }
        return null;
    }

    private OptionMap getOptionMapFromProperties(final Properties properties, final String propertyPrefix) {
        final ClassLoader classLoader = getClientClassLoader();
        final OptionMap.Builder optionMapBuilder = OptionMap.builder().parseAll(properties, propertyPrefix, classLoader);
        final OptionMap optionMap = optionMapBuilder.getMap();
        logger.debug(propertyPrefix + " has the following options " + optionMap);
        return optionMap;
    }

    /**
     * Merges the passed <code>defaults</code> and the <code>overrides</code> to return a combined
     * {@link OptionMap}. If the passed <code>overrides</code> has a {@link org.xnio.Option} for
     * which matches the one in <code>defaults</code> then the default option value is ignored and instead the
     * overridden one is added to the combined {@link OptionMap}. If however, the <code>overrides</code> doesn't
     * contain a option which is present in the <code>defaults</code>, then the default option is added to the
     * combined {@link OptionMap}
     *
     * @param defaults  The default options
     * @param overrides The overridden options
     * @return
     */
    private OptionMap mergeWithDefaults(final OptionMap defaults, final OptionMap overrides) {
        // copy all the overrides
        final OptionMap.Builder combinedOptionsBuilder = OptionMap.builder().addAll(overrides);
        // Skip all the defaults which have been overridden and just add the rest of the defaults
        // to the combined options
        for (final Option defaultOption : defaults) {
            if (combinedOptionsBuilder.getMap().contains(defaultOption)) {
                continue;
            }
            final Object defaultValue = defaults.get(defaultOption);
            combinedOptionsBuilder.set(defaultOption, defaultValue);
        }
        final OptionMap combinedOptions = combinedOptionsBuilder.getMap();
        if (logger.isTraceEnabled()) {
            logger.trace("Options " + overrides + " have been merged with defaults " + defaults + " to form " + combinedOptions);
        }
        return combinedOptions;
    }

    /**
     * If {@link Thread#getContextClassLoader()} is null then returns the classloader which loaded
     * {@link InitialContextFactory}. Else returns the {@link Thread#getContextClassLoader()}
     *
     * @return
     */
    private static ClassLoader getClientClassLoader() {
        final ClassLoader tccl = SecurityActions.getContextClassLoader();
        if (tccl != null) {
            return tccl;
        }
        return InitialContextFactory.class.getClassLoader();
    }

    private Properties findAndCreateClientProperties(final Hashtable<?, ?> env) {
        // First load the props file if it exists
        Properties props = findClientProperties();
        if(props == null) {
            props = new Properties();
        }
        // Now override with naming env entries
        for(Map.Entry<?, ?> entry : env.entrySet()) {
            if(entry.getKey() instanceof String && entry.getValue() instanceof String) {
                props.setProperty((String)entry.getKey(), (String)entry.getValue());
            }
        }
        return props;
    }

    private Properties findClientProperties() {
        final ClassLoader classLoader = getClientClassLoader();
        logger.debug("Looking for " + CLIENT_PROPS_FILE_NAME + " using classloader " + classLoader);
        // find from classloader
        final InputStream clientPropsInputStream = classLoader.getResourceAsStream(CLIENT_PROPS_FILE_NAME);
        if (clientPropsInputStream != null) {
            logger.debug("Found " + CLIENT_PROPS_FILE_NAME + " using classloader " + classLoader);
            final Properties clientProps = new Properties();
            try {
                clientProps.load(clientPropsInputStream);
                return clientProps;

            } catch (IOException e) {
                throw new RuntimeException("Could not load " + CLIENT_PROPS_FILE_NAME, e);
            }
        }
        return null;
    }

    // TODO: This is a hack for now, till we have a way to configure callback handlers
    // or other mechanism via the EJB client configuration file for connection creation
    private class AnonymousCallbackHandler implements CallbackHandler {
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback current : callbacks) {
                if (current instanceof NameCallback) {
                    NameCallback ncb = (NameCallback) current;
                    ncb.setName("anonymous");
                } else {
                    throw new UnsupportedCallbackException(current);
                }
            }
        }
    }

    private class AuthenticationCallbackHandler implements CallbackHandler {

        private final String realm;
        private final String username;
        private final char[] password;

        private AuthenticationCallbackHandler(final String username, final char[] password, final String realm) {
            this.username = username;
            this.password = password;
            this.realm = realm;
        }

        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback current : callbacks) {
                if (current instanceof RealmCallback) {
                    RealmCallback rcb = (RealmCallback) current;
                    if (realm == null) {
                        String defaultText = rcb.getDefaultText();
                        rcb.setText(defaultText); // For now just use the realm suggested.
                    } else {
                        rcb.setText(realm);
                    }
                } else if (current instanceof NameCallback) {
                    NameCallback ncb = (NameCallback) current;
                    ncb.setName(username);
                } else if (current instanceof PasswordCallback) {
                    PasswordCallback pcb = (PasswordCallback) current;
                    pcb.setPassword(password);
                } else {
                    throw new UnsupportedCallbackException(current);
                }
            }
        }
    }

}
