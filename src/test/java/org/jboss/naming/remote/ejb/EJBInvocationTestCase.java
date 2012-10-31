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

package org.jboss.naming.remote.ejb;

import org.jboss.ejb.client.EJBClient;
import org.jboss.ejb.client.StatelessEJBLocator;
import org.jboss.logging.Logger;
import org.jboss.naming.remote.MockContext;
import org.jboss.naming.remote.TestUtils;
import org.jboss.naming.remote.common.ejb.DummyEJBServer;
import org.jboss.naming.remote.common.ejb.EchoBean;
import org.jboss.naming.remote.common.ejb.EchoRemote;
import org.jboss.naming.remote.server.RemoteNamingService;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.Remoting;
import org.jboss.remoting3.remote.RemoteConnectionProviderFactory;
import org.jboss.remoting3.spi.NetworkServerProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xnio.OptionMap;
import org.xnio.Xnio;

import javax.naming.Context;
import javax.naming.InitialContext;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * Tests that invocations on EJB proxies via remote naming lookup works as expected
 *
 * @author Jaikiran Pai
 */
public class EJBInvocationTestCase {

    private static final Logger logger = Logger.getLogger(EJBInvocationTestCase.class);

    private static RemoteNamingService server;
    private static DummyEJBServer dummyEJBServer;

    private static final Context localContext = new MockContext();

    @BeforeClass
    public static void beforeClass() throws Exception {
        final Xnio xnio = Xnio.getInstance();
        final Endpoint endpoint = Remoting.createEndpoint("RemoteNaming", xnio, OptionMap.EMPTY);
        endpoint.addConnectionProvider("remote", new RemoteConnectionProviderFactory(), OptionMap.EMPTY);

        final NetworkServerProvider nsp = endpoint.getConnectionProviderInterface("remote", NetworkServerProvider.class);
        final SocketAddress bindAddress = new InetSocketAddress("localhost", 7999);
        final OptionMap serverOptions = TestUtils.createOptionMap();

        nsp.createServer(bindAddress, serverOptions, new TestUtils.DefaultAuthenticationHandler(), null);
        server = new RemoteNamingService(localContext, Executors.newFixedThreadPool(10));
        server.start(endpoint);

        // register the EJB server
        dummyEJBServer = new DummyEJBServer(endpoint);
        dummyEJBServer.start();

    }

    @AfterClass
    public static void afterClass() throws Exception {
        dummyEJBServer.stop();
        server.stop();
    }

    @Before
    public void beforeTest() {
        dummyEJBServer.register("my-app", "my-module", "", EchoBean.class.getSimpleName(), new EchoBean());
    }

    @After
    public void afterTest() {
        dummyEJBServer.unregister("my-app", "my-module", "", EchoBean.class.getSimpleName());
    }

    /**
     * Tests that a remote naming context created by using <code>jboss.naming.ejb.context=true</code> can be used
     * to do EJB lookup and invocations.
     *
     * @throws Exception
     */
    @Test
    public void testEJBInvocation() throws Exception {
        final StatelessEJBLocator<EchoRemote> statelessEJBLocator = new StatelessEJBLocator<EchoRemote>(EchoRemote.class, "my-app", "my-module", EchoBean.class.getSimpleName(), "");
        final EchoRemote bean = EJBClient.createProxy(statelessEJBLocator);
        final String jndiName = "ejb-invocation-test";
        localContext.bind(jndiName, bean);

        final Properties env = new Properties();
        env.put(Context.INITIAL_CONTEXT_FACTORY, org.jboss.naming.remote.client.InitialContextFactory.class.getName());
        env.put(Context.PROVIDER_URL, "remote://localhost:7999");
        env.put("jboss.naming.client.ejb.context", true);
        final Context context = new InitialContext(env);

        try {
            final Object returnedProxy = context.lookup(jndiName);
            Assert.assertTrue("Object returned by remote naming lookup: " + returnedProxy + " is not an EJB proxy", EJBClient.isEJBProxy(returnedProxy));
            // make sure the proxy is of the business interface type
            Assert.assertTrue("EJB proxy returned by remote naming lookup is not of type: " + EchoRemote.class, returnedProxy instanceof EchoRemote);
            // invoke on the bean proxy
            final String message = "Gangnam Style!!!";
            final String echo = ((EchoRemote) returnedProxy).echo(message);
            Assert.assertEquals("Unexpected echo message from the EJB proxy returned by remote naming lookup", message, echo);
        } finally {
            context.close();
            localContext.unbind(jndiName);
        }
    }


    /**
     * Tests that a remote naming context created by using <code>jboss.naming.ejb.context=false</code> fails
     * when used for EJB invocations
     *
     * @throws Exception
     */
    @Test
    public void testEJBInvocationWithoutEJBClientContext() throws Exception {
        final StatelessEJBLocator<EchoRemote> statelessEJBLocator = new StatelessEJBLocator<EchoRemote>(EchoRemote.class, "my-app", "my-module", EchoBean.class.getSimpleName(), "");
        final EchoRemote bean = EJBClient.createProxy(statelessEJBLocator);
        final String jndiName = "ejb-invocation-test-without-ejb-client-context";
        localContext.bind(jndiName, bean);


        final Properties env = new Properties();
        env.put(Context.INITIAL_CONTEXT_FACTORY, org.jboss.naming.remote.client.InitialContextFactory.class.getName());
        env.put(Context.PROVIDER_URL, "remote://localhost:7999");
        env.put("jboss.naming.client.ejb.context", false);
        final Context context = new InitialContext(env);

        try {
            final Object returnedProxy = context.lookup(jndiName);
            Assert.assertTrue("Object returned by remote naming lookup: " + returnedProxy + " is not an EJB proxy", EJBClient.isEJBProxy(returnedProxy));
            // make sure the proxy is of the business interface type
            Assert.assertTrue("EJB proxy returned by remote naming lookup is not of type: " + EchoRemote.class, returnedProxy instanceof EchoRemote);
            // invoke on the bean proxy. MUST fail since the EJB client context isn't setup for this JNDI context
            final String message = "Gangnam Style!!!";
            try {
                final String echo = ((EchoRemote) returnedProxy).echo(message);
                Assert.fail("Invocation on an EJB proxy without any EJB client context was expected to fail, but it didn't");
            } catch (IllegalStateException ise) {
                // expected
                logger.debug("Got the expected exception while invoking on an EJB proxy without any backing EJB client context", ise);
            }
        } finally {
            context.close();
            localContext.unbind(jndiName);
        }
    }
}
