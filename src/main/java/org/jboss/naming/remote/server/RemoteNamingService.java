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

package org.jboss.naming.remote.server;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Executor;
import javax.naming.Context;
import org.jboss.logging.Logger;
import org.jboss.naming.remote.Constants;
import static org.jboss.naming.remote.Constants.NAMING;
import org.jboss.naming.remote.protocol.CancellableDataOutputStream;
import org.jboss.naming.remote.protocol.Versions;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.OpenListener;
import org.jboss.remoting3.Registration;
import org.xnio.IoUtils;
import org.xnio.OptionMap;
import org.xnio.channels.AcceptingChannel;
import org.xnio.channels.ConnectedStreamChannel;

/**
 * @author John Bailey
 */
public class RemoteNamingService {
    private static final Logger log = Logger.getLogger(RemoteNamingService.class);

    private Endpoint endpoint;
    private Registration registration;
    private AcceptingChannel<? extends ConnectedStreamChannel> server;
    private final Context localContext;

    private final Executor executor;

    public RemoteNamingService(final Context localContext, final Executor executor, final Endpoint endpoint) {
        this.localContext = localContext;
        this.executor = executor;
        this.endpoint = endpoint;
    }

    public void start() throws IOException {
        registration = endpoint.registerService(Constants.CHANNEL_NAME, new ChannelOpenListener(), OptionMap.EMPTY);
    }

    private class ChannelOpenListener implements OpenListener {

        public void channelOpened(Channel channel) {
            log.trace("Channel Opened");
            channel.addCloseHandler(new ChannelCloseHandler());
            try {
                writeHeader(channel);
                channel.receiveMessage(new ClientVersionReceiver());
            } catch (IOException e) {
                log.error("Unable to send header, closing channel", e);
                IoUtils.safeClose(channel);
            }
        }

        public void registrationTerminated() {
        }
    }

    private void writeHeader(final Channel channel) throws IOException {
        CancellableDataOutputStream dos = new CancellableDataOutputStream(channel.writeMessage());
        try {
            dos.write(Constants.NAMING);
            byte[] versions = Versions.getSupportedVersions();
            dos.write(versions.length);
            dos.write(versions);
        } catch (IOException e) {
            dos.cancel();
            throw e;
        } finally {
            dos.close();
        }
    }

    private class ClientVersionReceiver implements Channel.Receiver {
        public void handleMessage(Channel channel, MessageInputStream messageInputStream) {
            DataInputStream dis = new DataInputStream(messageInputStream);
            try {
                byte[] namingHeader = new byte[6];
                dis.read(namingHeader);
                if (!Arrays.equals(namingHeader, NAMING)) {
                    throw new IOException("Invalid leading bytes in header.");
                }
                byte version = dis.readByte();
                log.debugf("Chosen version 0x0%d", version);

                Versions.getRemoteNamingServer(version, channel, RemoteNamingService.this);
            } catch (IOException e) {
                log.error("Error determining version selected by client.");
            } finally {
                IoUtils.safeClose(dis);
            }
        }

        public void handleError(final Channel channel, final IOException e) {
        }

        public void handleEnd(Channel channel) {
        }

    }

    private class ChannelCloseHandler implements CloseHandler<Channel> {

        public void handleClose(Channel channel, IOException e) {
            log.debug("Server handleClose");
            // TODO - Perform Clean Up - possibly notification registrations and even connection registrations.
        }
    }

    public void stop() throws IOException {
        if (server != null) {
            server.close();
        }

        if (endpoint != null) {
            endpoint.close();
        }

    }

    public Context getLocalContext() {
        return localContext;
    }

    public Executor getExecutor() {
        return executor;
    }
}

