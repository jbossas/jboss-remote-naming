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
package org.jboss.naming.remote.protocol.v1;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NamingException;
import org.jboss.logging.Logger;
import static org.jboss.naming.remote.client.ClientUtil.namingException;
import org.jboss.naming.remote.client.RemoteNamingStore;
import org.jboss.naming.remote.protocol.ProtocolCommand;
import static org.jboss.naming.remote.protocol.v1.WriteUtil.write;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.MessageInputStream;
import org.xnio.IoUtils;

/**
 * @author John Bailey
 */
public class RemoteNamingStoreV1 implements RemoteNamingStore {
    private static final Logger log = Logger.getLogger(RemoteNamingStoreV1.class);

    private final ExecutorService executor = Executors.newCachedThreadPool(new DaemonThreadFactory());
    private final Channel channel;

    public RemoteNamingStoreV1(final Channel channel) {
        this.channel = channel;
    }

    void start() throws IOException {
        sendVersionHeader();
        channel.receiveMessage(new MessageReceiver());
    }

    private void sendVersionHeader() throws IOException {
        write(channel, new WriteUtil.Writer() {
            public void write(DataOutput output) throws IOException {
                output.write(org.jboss.naming.remote.Constants.NAMING);
                output.writeByte(VersionOne.getVersionIdentifier());
            }
        });
    }

    public Object lookup(final Name name) throws NamingException {
        try {
            return Protocol.LOOKUP.execute(channel, name);
        } catch (IOException e) {
            throw namingException("Failed to execute lookup for [" + name + "]", e);
        }
    }

    public void bind(final Name name, final Object object) throws NamingException {
        try {
            Protocol.BIND.execute(channel, name, object);
        } catch (IOException e) {
            throw namingException("Failed to execute bind for [" + name + ", " + object + "]", e);
        }
    }

    public void rebind(Name name, Object object) throws NamingException {
        try {
            Protocol.REBIND.execute(channel, name, object);
        } catch (IOException e) {
            throw namingException("Failed to execute rebind for [" + name + ", " + object + "]", e);
        }
    }

    public List<NameClassPair> list(Name name) throws NamingException {
        try {
            return Protocol.LIST.execute(channel, name);
        } catch (IOException e) {
            throw namingException("Failed to execute list for [" + name + "]", e);
        }
    }

    public List<Binding> listBindings(final Name name) throws NamingException {
        try {
            return Protocol.LIST_BINDINGS.execute(channel, name);
        } catch (IOException e) {
            throw namingException("Failed to execute list bindings for [" + name + "]", e);
        }
    }

    public void unbind(final Name name) throws NamingException {
        try {
            Protocol.UNBIND.execute(channel, name);
        } catch (IOException e) {
            throw namingException("Failed to execute unbind for [" + name + "]", e);
        }
    }

    public void rename(final Name name, final Name newName) throws NamingException {
        try {
            Protocol.RENAME.execute(channel, name, newName);
        } catch (IOException e) {
            throw namingException("Failed to execute rename for [" + name + ", " + newName + "]", e);
        }
    }

    public Context createSubcontext(final Name name) throws NamingException {
        try {
            return Protocol.CREATE_SUBCONTEXT.execute(channel, name);
        } catch (IOException e) {
            throw namingException("Failed to execute createSubcontext for [" + name + "]", e);
        }
    }

    public void destroySubcontext(final Name name) throws NamingException {
        try {
            Protocol.DESTROY_SUBCONTEXT.execute(channel, name);
        } catch (IOException e) {
            throw namingException("Failed to execute destroySubcontext for [" + name + "]", e);
        }
    }

    public Object lookupLink(final Name name) throws NamingException {
        try {
            return Protocol.LOOKUP_LINK.execute(channel, name);
        } catch (IOException e) {
            throw namingException("Failed to execute lookupLink for [" + name + "]", e);
        }
    }

    public void close() throws NamingException {
        // shutdown the executor service
        try {
            if (this.executor != null) {
                this.executor.shutdown();
            }
        } catch (Exception e) {
            // log and ignore
            log.debug("Could not shutdown executor service", e);
        }
        try {
            channel.close();
        } catch (IOException e) {
            throw namingException("Failed to close remote naming store", e);
        }
    }

    private class MessageReceiver implements Channel.Receiver {
        public void handleMessage(Channel channel, MessageInputStream message) {
            final DataInputStream dis = new DataInputStream(message);
            try {
                byte messageId = dis.readByte();
                final int correlationId = dis.readInt();
                log.tracef("Message Received id(%h), correlationId(%d)", messageId, correlationId);

                final ProtocolCommand command = Protocol.forId(messageId);
                if (command != null) {
                    executor.execute(new Runnable() {
                        public void run() {
                            try {
                                command.handleClientMessage(dis, correlationId, RemoteNamingStoreV1.this);
                            } catch (IOException e) {
                                log.error(e);
                            } finally {
                                IoUtils.safeClose(dis);
                            }
                        }

                    });

                } else {
                    throw new IOException("Unrecognised Message ID");
                }
            } catch (IOException e) {
                log.error(e);
                IoUtils.safeClose(dis);
            } finally {
                channel.receiveMessage(this);
            }
        }

        public void handleError(final Channel channel, final IOException error) {
            log.errorf(error, "Closing channel %s due to an error", channel);
            try {
                channel.close();
            } catch (IOException ignore) {
            }
        }

        public void handleEnd(final Channel channel) {
            log.errorf("Channel end notification received, closing channel %s", channel);
            try {
                channel.close();
            } catch (IOException ignore) {
            }
        }

    }

    /**
     * A thread factory which creates daemon threads which will be used for
     * creating connections to cluster nodes
     */
    private static class DaemonThreadFactory implements ThreadFactory {

        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        DaemonThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            namePrefix = "naming-client-message-receiver-" +
                    poolNumber.getAndIncrement() +
                    "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            t.setDaemon(true);
            return t;
        }
    }
}
