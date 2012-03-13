package org.jboss.naming.remote;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Properties;
import java.util.concurrent.Executors;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.naming.remote.server.RemoteNamingService;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.Remoting;
import org.jboss.remoting3.remote.RemoteConnectionProviderFactory;
import org.jboss.remoting3.spi.NetworkServerProvider;
import org.junit.Test;
import org.xnio.OptionMap;
import org.xnio.Xnio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Stuart Douglas
 */
public class FailoverConnectionTest {


    public static final String SERVER = "Server-Port";

    public static Endpoint createServer(int port) throws Exception {
        Context localContext = new MockContext();
        localContext.bind("serverId", SERVER + port);
        final Xnio xnio = Xnio.getInstance();
        final Endpoint endpoint = Remoting.createEndpoint("RemoteNaming", xnio, OptionMap.EMPTY);
        endpoint.addConnectionProvider("remote", new RemoteConnectionProviderFactory(), OptionMap.EMPTY);

        final NetworkServerProvider nsp = endpoint.getConnectionProviderInterface("remote", NetworkServerProvider.class);
        final SocketAddress bindAddress = new InetSocketAddress("localhost", port);
        final OptionMap serverOptions = TestUtils.createOptionMap();

        nsp.createServer(bindAddress, serverOptions, new TestUtils.DefaultAuthenticationHandler(), null);
        RemoteNamingService server = new RemoteNamingService(localContext, Executors.newFixedThreadPool(10));
        server.start(endpoint);
        return endpoint;
    }


    @Test
    public void testHaContext() throws Exception {

        Endpoint server1 = createServer(7999);
        Endpoint server2 = createServer(8999);
        Endpoint server3 = null;
        try {
            Properties env = new Properties();
            env.put(Context.INITIAL_CONTEXT_FACTORY, org.jboss.naming.remote.client.InitialContextFactory.class.getName());
            env.put(Context.PROVIDER_URL, "remote://localhost:7999,remote://localhost:8999,remote://localhost:9999");
            env.put("jboss.naming.client.ejb.context", "false");
            InitialContext context = new InitialContext(env);

            assertEquals(SERVER + 7999, context.lookup("serverId"));
            assertEquals(SERVER + 7999, context.lookup("serverId"));
            assertEquals(SERVER + 7999, context.lookup("serverId"));
            server1.close();
            assertEquals(SERVER + 8999, context.lookup("serverId"));
            server2.close();
            try {
                context.lookup("serverId");
                fail("Excepted a NamingException");
            } catch (NamingException exception) {

            }
            server3 = createServer(9999);
            assertEquals(SERVER + 9999, context.lookup("serverId"));
            server1 = createServer(7999);
            assertEquals(SERVER + 9999, context.lookup("serverId"));
            assertEquals(SERVER + 9999, context.lookup("serverId"));
            server3.close();
            assertEquals(SERVER + 7999, context.lookup("serverId"));
            context.close();
        } finally {
            if (server1 != null) {
                server1.close();
            }
            if (server2 != null) {
                server2.close();
            }
            if (server3 != null) {
                server3.close();
            }
        }
    }


}
