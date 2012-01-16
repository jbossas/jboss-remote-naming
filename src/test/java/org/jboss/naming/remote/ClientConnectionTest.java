/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
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
package org.jboss.naming.remote;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.LinkRef;
import javax.naming.NameClassPair;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.SaslException;
import org.jboss.naming.remote.client.RemoteContext;
import org.jboss.naming.remote.server.DefaultRemoteNamingServerLogger;
import org.jboss.naming.remote.server.RemoteNamingService;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.Remoting;
import org.jboss.remoting3.remote.RemoteConnectionProviderFactory;
import org.jboss.remoting3.security.ServerAuthenticationProvider;
import org.jboss.remoting3.spi.NetworkServerProvider;
import org.jboss.sasl.callback.VerifyPasswordCallback;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xnio.OptionMap;
import static org.xnio.Options.SASL_MECHANISMS;
import static org.xnio.Options.SASL_POLICY_NOANONYMOUS;
import static org.xnio.Options.SASL_PROPERTIES;
import static org.xnio.Options.SSL_ENABLED;
import org.xnio.Property;
import org.xnio.Sequence;
import org.xnio.Xnio;
import org.xnio.channels.AcceptingChannel;

/**
 * @author John Bailey
 */
public class ClientConnectionTest {
    private static RemoteNamingService server;
    private static Context remoteContext;

    private static final Context localContext = new MockContext();

    @BeforeClass
    public static void beforeClass() throws Exception {
        final Xnio xnio = Xnio.getInstance();
        final Endpoint endpoint = Remoting.createEndpoint("RemoteNaming", xnio, OptionMap.EMPTY);
        endpoint.addConnectionProvider("remote", new RemoteConnectionProviderFactory(), OptionMap.EMPTY);

        final NetworkServerProvider nsp = endpoint.getConnectionProviderInterface("remote", NetworkServerProvider.class);
        final SocketAddress bindAddress = new InetSocketAddress("localhost", 7999);
        final OptionMap serverOptions = createOptionMap();

        final AcceptingChannel channel = nsp.createServer(bindAddress, serverOptions, new DefaultAuthenticationHandler(), null);
        server = new RemoteNamingService(localContext, Executors.newFixedThreadPool(10));
        server.start(endpoint);

        Properties env = new Properties();
        env.put(Context.INITIAL_CONTEXT_FACTORY, org.jboss.naming.remote.client.InitialContextFactory.class.getName());
        env.put(Context.PROVIDER_URL, "remote://localhost:7999");
        env.put("jboss.naming.client.remote.connectionprovider.create.options.org.xnio.Options.SSL_ENABLED", "false");
        env.put("jboss.naming.client.connect.options.org.xnio.Options.SASL_POLICY_NOANONYMOUS", "false");
        remoteContext = new InitialContext(env);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        remoteContext.close();
        server.stop();
    }

    @Test
    public void testLookup() throws Exception {
        localContext.bind("test", "TestValue");
        assertEquals("TestValue", remoteContext.lookup("test"));
        localContext.unbind("test");
    }

    @Test
    public void testLookupNotFound() throws Exception {
        try {
            remoteContext.lookup("test");
            fail("Should have thrown NameNotFound");
        } catch (NameNotFoundException expected) {
        }
    }

    @Test
    public void testLookupContext() throws Exception {
        localContext.createSubcontext("test");
        try {
            final Object result = remoteContext.lookup("test");
            assertNotNull(result);
            assertTrue(result instanceof RemoteContext);
        } finally {
            localContext.destroySubcontext("test");
        }
    }

    @Test
    public void testLookupInSubContext() throws Exception {
        localContext.createSubcontext("test").bind("nested", "TestValue");
        try {
            final Object result = remoteContext.lookup("test");
            assertNotNull(result);
            assertTrue(result instanceof RemoteContext);
            assertEquals("TestValue", Context.class.cast(result).lookup("nested"));
        } finally {
            localContext.destroySubcontext("test");
        }
    }

    @Test
    public void testLookupNested() throws Exception {
        localContext.createSubcontext("test").bind("nested", "TestValue");
        try {
            assertEquals("TestValue", remoteContext.lookup("test/nested"));
        } finally {
            localContext.destroySubcontext("test");
        }
    }

    @Test
    public void testBind() throws Exception {
        remoteContext.bind("test", "TestValue");
        assertEquals("TestValue", localContext.lookup("test"));
        localContext.unbind("test");
    }

    @Test
    public void testBindNested() throws Exception {
        localContext.createSubcontext("test");
        remoteContext.bind("test/nested", "TestValue");
        assertEquals("TestValue", localContext.lookup("test/nested"));
        localContext.destroySubcontext("test");
    }

    @Test
    public void testBindInvalidContext() throws Exception {
        try {
            remoteContext.bind("test/nested", "TestValue");
            fail("Should have thrown NameNotFound");
        } catch (NameNotFoundException expected) {
        }
    }

    @Test
    public void testRebind() throws Exception {
        localContext.bind("test", "InitialValue");
        remoteContext.rebind("test", "TestValue");
        assertEquals("TestValue", localContext.lookup("test"));
        localContext.unbind("test");
    }

    @Test
    public void testRebindNested() throws Exception {
        localContext.createSubcontext("test");
        localContext.bind("test/nested", "Initial");
        remoteContext.rebind("test/nested", "TestValue");
        assertEquals("TestValue", localContext.lookup("test/nested"));
        localContext.destroySubcontext("test");
    }

    @Test
    public void testRebindInvalidContext() throws Exception {
        try {
            remoteContext.rebind("test/nested", "TestValue");
            fail("Should have thrown NameNotFound");
        } catch (NameNotFoundException expected) {
        }
    }


    @Test
    public void testList() throws Exception {
        localContext.bind("test1", "TestValue1");
        localContext.bind("test2", new Object());
        localContext.bind("test3", "TestValue3");
        localContext.bind("test4", "TestValue4");
        localContext.createSubcontext("test5");

        final NamingEnumeration<NameClassPair> entries = remoteContext.list("test");
        final Map<String, String> expected = new HashMap<String, String>();
        expected.put("test1", String.class.getName());
        expected.put("test2", Object.class.getName());
        expected.put("test3", String.class.getName());
        expected.put("test4", String.class.getName());
        expected.put("test5", Context.class.getName());

        while (entries.hasMore()) {
            final NameClassPair pair = entries.next();
            assertTrue("Unexpected pair: " + pair.getName(), expected.containsKey(pair.getName()));
            assertEquals(expected.get(pair.getName()), pair.getClassName());
            expected.remove(pair.getName());
        }

        assertTrue(expected.isEmpty());

        localContext.unbind("test1");
        localContext.unbind("test2");
        localContext.unbind("test3");
        localContext.unbind("test4");
        localContext.unbind("test5");
    }

    @Test
    public void testListBindings() throws Exception {
        localContext.bind("test1", "TestValue1");
        final Object value2 = new Integer(1);
        localContext.bind("test2", value2);
        localContext.bind("test3", "TestValue3");
        localContext.bind("test4", "TestValue4");
        final Context context = localContext.createSubcontext("test5");

        final NamingEnumeration<Binding> entries = remoteContext.listBindings("");
        final Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("test1", "TestValue1");
        expected.put("test2", value2);
        expected.put("test3", "TestValue3");
        expected.put("test4", "TestValue4");
        expected.put("test5", context);

        while (entries.hasMore()) {
            final Binding binding = entries.next();
            assertTrue("Unexpected pair: " + binding.getName(), expected.containsKey(binding.getName()));
            if (!binding.getClassName().equals(Context.class.getName())) {
                assertEquals(expected.get(binding.getName()), binding.getObject());
            }
            expected.remove(binding.getName());
        }

        assertTrue(expected.isEmpty());

        localContext.unbind("test1");
        localContext.unbind("test2");
        localContext.unbind("test3");
        localContext.unbind("test4");
        localContext.unbind("test5");
    }

    @Test
    public void testLookupFromListBindings() throws Exception {
        localContext.createSubcontext("test").bind("nested", "TestValue");

        final NamingEnumeration<Binding> entries = remoteContext.listBindings("");
        assertTrue(entries.hasMore());
        final Binding binding = entries.next();
        assertEquals("test", binding.getName());
        assertEquals(Context.class.getName(), binding.getClassName());
        assertEquals("TestValue", Context.class.cast(binding.getObject()).lookup("nested"));
        assertFalse(entries.hasMore());

        localContext.unbind("test");
    }


    @Test
    public void testUnbind() throws Exception {
        localContext.bind("test", "TestValue1");
        remoteContext.unbind("test");
        try {
            localContext.lookup("test");
            fail("Should have thrown NameNotFound");
        } catch (NameNotFoundException e) {
        }
    }


    @Test
    public void testUnbindNotFound() throws Exception {
        try {
            remoteContext.unbind("test");
            fail("Should have thrown NameNotFound");
        } catch (NameNotFoundException e) {
        }
    }


    @Test
    public void testRename() throws Exception {
        localContext.bind("test", "TestValue1");
        remoteContext.rename("test", "test2");
        try {
            localContext.lookup("test");
            fail("Should have thrown NameNotFound");
        } catch (NameNotFoundException e) {
        }
        assertEquals("TestValue1", localContext.lookup("test2"));
        localContext.unbind("test2");
    }

    @Test
    public void testRenameNotFound() throws Exception {
        try {
            remoteContext.rename("test", "test2");
            fail("Should have thrown NameNotFound");
        } catch (NameNotFoundException e) {
        }
    }

    @Test
    public void testCreateSubcontext() throws Exception {
        remoteContext.createSubcontext("test");
        assertTrue(localContext.lookup("test") instanceof Context);
        localContext.unbind("test");

    }

    @Test
    public void testDestroySubcontext() throws Exception {
        localContext.createSubcontext("test");
        remoteContext.destroySubcontext("test");
        try {
            remoteContext.lookup("test");
            fail("Should have thrown NameNotFound");
        } catch (NameNotFoundException e) {
        }
    }

    @Test
    public void testLookupLink() throws Exception {
        localContext.bind("test", "testValue");
        localContext.bind("link", new LinkRef("./test"));
        assertTrue(remoteContext.lookupLink("link") instanceof LinkRef);
        localContext.unbind("test");
        localContext.unbind("link");
    }

    public static final String ANONYMOUS = "ANONYMOUS";
    public static final String DIGEST_MD5 = "DIGEST-MD5";
    public static final String JBOSS_LOCAL_USER = "JBOSS-LOCAL-USER";
    public static final String PLAIN = "PLAIN";
    private static final String DOLLAR_LOCAL = "$local";
    private static final String REALM = "Naming_Test_Realm";

    private static OptionMap createOptionMap() {
        OptionMap.Builder builder = OptionMap.builder();
        builder.set(SSL_ENABLED, false);
        builder.set(SASL_MECHANISMS, Sequence.<String>of(ANONYMOUS));
        builder.set(SASL_PROPERTIES, Sequence.<Property>empty());
        builder.set(SASL_POLICY_NOANONYMOUS, false);

        return builder.getMap();
    }

    private static class DefaultAuthenticationHandler implements ServerAuthenticationProvider {
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
