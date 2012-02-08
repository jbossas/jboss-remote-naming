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
package org.jboss.naming.remote.client;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.naming.Context;
import javax.naming.NamingException;
import org.jboss.logging.Logger;
import static org.jboss.naming.remote.Constants.NAMING;
import org.jboss.naming.remote.protocol.Versions;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.MessageInputStream;
import org.xnio.AbstractIoFuture;
import org.xnio.IoFuture;
import org.xnio.IoUtils;

/**
 * The VersionedContextFactory to negotiate the version on the client side and return an appropriate Context for
 * the negotiated version.
 *
 * @author John Bailey
 * @author <a href="mailto:darran.lofthouse@jboss.com">Darran Lofthouse</a>
 */
class RemoteContextFactory {

    private static final Logger log = Logger.getLogger(RemoteContextFactory.class);

    static Context createVersionedContext(final Channel channel, final Hashtable<String, Object> environment, final List<RemoteContext.CloseTask> closeTasks) throws IOException {
        IoFuture<byte[]> futureHeader = ClientVersionReceiver.getVersions(channel);
        IoFuture.Status result = futureHeader.await(5, TimeUnit.SECONDS);
        switch (result) {
            case DONE:
                break;
            case FAILED:
                throw futureHeader.getException();
            default:
                throw new IOException("Timeout out waiting for header, status=" + result.toString());
        }

        byte highest = 0x00;
        for (byte current : futureHeader.get()) {
            if (current > highest) {
                highest = current;
            }
        }
        final RemoteNamingStore store = Versions.getRemoteNamingStore(highest, channel);
        closeTasks.add(new RemoteContext.CloseTask() {
            public void close(boolean isFinalize) {
                try {
                    store.close();
                } catch (NamingException e) {
                    throw new RuntimeException("Failed to close remote naming store", e);
                }
            }
        });
        return new RemoteContext(store, environment, closeTasks);
    }

    /**
     * A Channel.Receiver to receive the list of versions supported by the remote server.
     */
    private static class ClientVersionReceiver implements org.jboss.remoting3.Channel.Receiver {

        private final VersionsIoFuture future;

        private ClientVersionReceiver(final VersionsIoFuture future) {
            this.future = future;
        }

        public static IoFuture<byte[]> getVersions(final Channel channel) {
            final VersionsIoFuture future = new VersionsIoFuture();
            channel.receiveMessage(new ClientVersionReceiver(future));
            return future;
        }

        /**
         * Verify the header received, confirm to the server the version selected, create the client channel receiver and assign
         * it to the channel.
         */
        public void handleMessage(final Channel channel, final MessageInputStream messageInputStream) {
            DataInputStream dis = new DataInputStream(messageInputStream);
            try {
                final int expectedHeaderLength = NAMING.length;
                final byte[] header = new byte[expectedHeaderLength];
                int read = dis.read(header);
                if (read < expectedHeaderLength || !Arrays.equals(header, NAMING)) {
                    throw new IOException("Invalid leading bytes in header.");
                }
                final int versionCount = dis.read();
                final byte[] versions = new byte[versionCount];
                read = dis.read(versions);
                if(read < versionCount) {
                    throw new IOException("Did not read all versions.");
                }
                future.setResult(versions);
            } catch (IOException e) {
                log.error("Unable to negotiate connection.", e);
                future.setException(e);
            } finally {
                IoUtils.safeClose(dis);
            }
        }

        public void handleError(final Channel channel, IOException e) {
            log.error("Error on channel", e);
            future.setException(e);
        }

        public void handleEnd(final Channel channel) {
            log.error("Channel ended.");
            future.setException(new IOException("Channel ended"));
        }
    }

    private static class VersionsIoFuture extends AbstractIoFuture<byte[]> {
        protected boolean setResult(byte[] result) {
            return super.setResult(result);
        }
        protected boolean setException(IOException exception) {
            return super.setException(exception);
        }
    }
}
