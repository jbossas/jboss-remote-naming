package org.jboss.naming.remote;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

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

import static org.junit.Assert.assertEquals;

/**
 * @author Stuart Douglas
 */
public class ConcurrentConnectionTest {

    private static RemoteNamingService server;

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
    }

    @AfterClass
    public static void afterClass() throws Exception {
        server.stop();
    }
    @Test
    public void multiThreadedStressTest() throws NamingException{
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        localContext.bind("test", "TestValue");
        try {
            final Future[] futures = new Future[1000];
            for (int i = 0; i < 1000; ++i) {
                futures[i] = executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Properties env = new Properties();
                            env.put(Context.INITIAL_CONTEXT_FACTORY, org.jboss.naming.remote.client.InitialContextFactory.class.getName());
                            env.put(Context.PROVIDER_URL, "remote://localhost:7999");
                            env.put("jboss.naming.client.ejb.context", "false");
                            InitialContext context = new InitialContext(env);
                            assertEquals("TestValue", context.lookup("test"));
                            context.close();
                        } catch (NamingException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
            for(int i = 0; i < 1000; ++i) {
                try {
                    futures[i].get();
                } catch (Exception e) {
                    throw new RuntimeException("Failed on invocation " + i, e);
                }
            }
        } finally {
            executorService.shutdownNow();
            localContext.unbind("test");
        }
    }

}
