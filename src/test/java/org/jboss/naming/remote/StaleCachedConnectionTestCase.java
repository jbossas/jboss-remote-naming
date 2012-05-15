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

package org.jboss.naming.remote;

import org.jboss.logging.Logger;
import org.jboss.naming.remote.server.RemoteNamingService;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.Remoting;
import org.jboss.remoting3.remote.RemoteConnectionProviderFactory;
import org.jboss.remoting3.spi.NetworkServerProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xnio.OptionMap;
import org.xnio.Xnio;
import org.xnio.channels.AcceptingChannel;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Properties;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * A test which ensures that the {@link org.jboss.naming.remote.client.NamingStoreCache} doesn't
 * return stale/closed connections during {@link Context} creation
 *
 * @author Jaikiran Pai
 */
public class StaleCachedConnectionTestCase {

    private static final Logger logger = Logger.getLogger(StaleCachedConnectionTestCase.class);

    private final Context localContext = new MockContext();
    private Endpoint endpoint;
    private AcceptingChannel server;
    private RemoteNamingService remoteNamingService;
    private boolean serverStopped;

    @Before
    public void beforeTest() throws Exception {
        this.startServer();
        localContext.bind("test", "TestValue");
    }

    @After
    public void afterTest() throws Exception {
        localContext.unbind("test");
        if (!serverStopped) {
            stopServer();
        }
    }

    private void startServer() throws Exception {
        final Xnio xnio = Xnio.getInstance();
        endpoint = Remoting.createEndpoint("RemoteNaming", xnio, OptionMap.EMPTY);
        endpoint.addConnectionProvider("remote", new RemoteConnectionProviderFactory(), OptionMap.EMPTY);
        final NetworkServerProvider nsp = endpoint.getConnectionProviderInterface("remote", NetworkServerProvider.class);
        final SocketAddress bindAddress = new InetSocketAddress("localhost", 7999);
        final OptionMap serverOptions = TestUtils.createOptionMap();

        server = nsp.createServer(bindAddress, serverOptions, new TestUtils.DefaultAuthenticationHandler(), null);
        remoteNamingService = new RemoteNamingService(localContext, Executors.newFixedThreadPool(10));
        remoteNamingService.start(endpoint);

        serverStopped = false;
    }

    /**
     * - We first create a {@link Context}, do a lookup and then stop the server.
     * - Next we do the lookup and it's expected to fail, since the server is down.
     * - We restart the server and then recreate the {@link Context} and do the lookup again. The lookup is expected
     * to succeed
     *
     * @throws Exception
     */
    @Test
    public void testConnectionCaching() throws Exception {
        Properties env = new Properties();
        env.put(Context.INITIAL_CONTEXT_FACTORY, org.jboss.naming.remote.client.InitialContextFactory.class.getName());
        env.put(Context.PROVIDER_URL, "remote://localhost:7999");
        env.put("jboss.naming.client.ejb.context", "false");
        final InitialContext context = new InitialContext(env);
        assertEquals("TestValue", context.lookup("test"));
        // now stop the server
        stopServer();
        logger.info("Stopped server");
        // lookup again, should fail since server is stopped
        try {
            context.lookup("test");
            fail("Lookup was expected to fail when server was down");
        } catch (NamingException ne) {
            // expected
        }
        // now restart the server
        startServer();
        // now create a context and lookup again, this should succeed
        final Context contextAfterServerStart = new InitialContext(env);
        final String lookupValueAfterServerRestart = (String) contextAfterServerStart.lookup("test");
        assertEquals("Unexpected lookup value after server was restarted", "TestValue", lookupValueAfterServerRestart);
    }

    private void stopServer() throws IOException {
        remoteNamingService.stop();
        server.close();
        endpoint.close();
        serverStopped = true;
    }

}
