package org.jboss.naming.remote.client;

import java.lang.reflect.Method;
import java.util.Hashtable;
import java.util.List;

import javax.naming.Context;
import javax.naming.NamingException;

import org.jboss.remoting3.Channel;
import org.jboss.remoting3.Connection;

import static org.jboss.naming.remote.client.ClientUtil.namingException;

/**
 * Utility class used to setup the channel and get the versioned {@link javax.naming.Context}
 *
 * @author Stuart Douglas
 */
public class ChannelSetup {

    public static Context createContext(final Channel channel, final Hashtable<?, ?> env, final List<RemoteContext.CloseTask> closeTasks) throws NamingException {
        try {

            final Object setupEJBClientContextProp = env.get(InitialContextFactory.SETUP_EJB_CONTEXT);
            final Boolean setupEJBClientContext;
            if (setupEJBClientContextProp instanceof String) {
                setupEJBClientContext = Boolean.parseBoolean((String) setupEJBClientContextProp);
            } else if (setupEJBClientContextProp instanceof Boolean) {
                setupEJBClientContext = (Boolean) setupEJBClientContextProp;
            } else {
                setupEJBClientContext = Boolean.FALSE;
            }
            if (setupEJBClientContext) {
                setupEjbContext(channel.getConnection(), closeTasks);
            }
            return RemoteContextFactory.createVersionedContext(channel, (Hashtable<String, Object>) env, closeTasks);
        } catch (Throwable t) {
            throw namingException("Failed to create remoting connection", t);
        }
    }


    /*
       Temporary hack to allow remote ejbs to share the remote connection used by naming.
    */
    private static void setupEjbContext(final Connection connection, final List<RemoteContext.CloseTask> closeTasks) {
        try {
            final ClassLoader classLoader = InitialContextFactory.class.getClassLoader();
            final Class<?> selectorClass = classLoader.loadClass("org.jboss.naming.remote.client.ejb.EjbClientContextSelector");
            final Method setup = selectorClass.getMethod("setupSelector", Connection.class);
            final Object previous = setup.invoke(null, connection);
            closeTasks.add(new RemoteContext.CloseTask() {
                public void close(final boolean isFinalize) {
                    try {
                        final Method reset = selectorClass.getMethod("resetSelector", classLoader.loadClass("org.jboss.ejb.client.ContextSelector"));
                        reset.invoke(null, previous);
                    } catch (Throwable t) {
                        throw new RuntimeException("Failed to reset EJB remote context", t);
                    }
                }
            });
        } catch (Throwable t) {
            throw new RuntimeException("Failed to setup EJB remote context", t);
        }
    }

}
