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

import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Properties;
import java.util.concurrent.Executors;

import javax.naming.AuthenticationException;
import javax.naming.CommunicationException;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.jboss.naming.remote.server.RemoteNamingService;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.Remoting;
import org.jboss.remoting3.remote.RemoteConnectionProviderFactory;
import org.jboss.remoting3.spi.NetworkServerProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.xnio.OptionMap;
import org.xnio.Xnio;

/**
 * @author Brad Maxwell
 */
public class NamingExceptionsTest {
    private static RemoteNamingService server;
    private static Context remoteContext;
    private static Context serversDownContext;

    private static final Context localContext = new MockContext();

    @BeforeClass
    public static void beforeClass() throws Exception {
        final Xnio xnio = Xnio.getInstance();
        final Endpoint endpoint = Remoting.createEndpoint("RemoteNaming", xnio, OptionMap.EMPTY);
        endpoint.addConnectionProvider("remote", new RemoteConnectionProviderFactory(), OptionMap.EMPTY);

        final NetworkServerProvider nsp = endpoint.getConnectionProviderInterface("remote", NetworkServerProvider.class);
        final SocketAddress bindAddress = new InetSocketAddress("localhost", 7999);
        final OptionMap serverOptions = TestUtils.createOptionNoAuthMechanismMap();

        nsp.createServer(bindAddress, serverOptions, new TestUtils.DefaultAuthenticationHandler(), null);
        server = new RemoteNamingService(localContext, Executors.newFixedThreadPool(10));
        server.start(endpoint);

        Properties env = new Properties();
        env.put(Context.INITIAL_CONTEXT_FACTORY, org.jboss.naming.remote.client.InitialContextFactory.class.getName());
        env.put(Context.PROVIDER_URL, "remote://localhost:8999, remote://localhost:7999");
        env.put("jboss.naming.client.ejb.context", "false");        
        // Since there one of the servers is not running, we will timeout quicker 
        env.put("jboss.naming.client.connect.timeout", "1000");
        remoteContext = new InitialContext(env);
        
        Properties serverDownEnv = new Properties();
        serverDownEnv.put(Context.INITIAL_CONTEXT_FACTORY, org.jboss.naming.remote.client.InitialContextFactory.class.getName());
        serverDownEnv.put(Context.PROVIDER_URL, "remote://localhost:8999");
        serverDownEnv.put("jboss.naming.client.ejb.context", "false");
        // Since there is no server running, we will timeout quicker 
        serverDownEnv.put("jboss.naming.client.connect.timeout", "1000");
        serversDownContext = new InitialContext(serverDownEnv);        
    }

    @AfterClass
    public static void afterClass() throws Exception {
        remoteContext.close();
        serversDownContext.close();
        server.stop();
    }

    @Test
    public void testCommunicationException() throws Exception {
        try {
        	// lower timeout to 1 sec instead of 5
            serversDownContext.lookup("test/server/down");
            fail("Should have thrown CommunicationException");
        } catch (CommunicationException expected) {
        }
    }
        
    @Test
    public void testAuthenticationException() throws Exception {
        try {
        	// 1 server is down, 1 is up but authentication will fail
            remoteContext.lookup("test/server/invalid/password");
            fail("Should have thrown AuthenticationException");
        } catch (AuthenticationException expected) {
        }
    }
}
