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
import java.io.IOException;
import javax.management.RuntimeMBeanException;
import org.jboss.logging.Logger;
import org.jboss.naming.remote.server.RemoteNamingServer;
import org.jboss.naming.remote.server.RemoteNamingService;
import org.jboss.naming.remote.protocol.ProtocolCommand;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.MessageInputStream;
import org.xnio.IoUtils;

/**
 * @author John Bailey
 */
public class RemoteNamingServerV1 implements RemoteNamingServer {
    private static final Logger log = Logger.getLogger(RemoteNamingServerV1.class);

    private final Channel channel;
    private final RemoteNamingService server;

    public RemoteNamingServerV1(final Channel channel, final RemoteNamingService remoteNamingServer) {
        this.channel = channel;
        this.server = remoteNamingServer;
    }

    public void start() {
        //server.connectionOpened(this);
        channel.receiveMessage(new MessageReciever());
    }

    public void stop() {
    }


    private class MessageReciever implements Channel.Receiver {
        public void handleMessage(final Channel channel, MessageInputStream message) {
            final DataInputStream dis = new DataInputStream(message);
            try {
                final byte messageId = dis.readByte();
                final int correlationId = dis.readInt();
                log.tracef("Message Received id(%h), correlationId(%d)", messageId, correlationId);

                final ProtocolCommand command = Protocol.forId(messageId);
                if (command != null) {
                    server.getExecutor().execute(new Runnable() {
                        public void run() {
                            try {
                                command.handleServerMessage(channel, dis, correlationId, server);
                            } catch (Throwable t) {
                                if (correlationId != 0x00) {
                                    Exception response;
                                    if (t instanceof IOException) {
                                        response = (Exception) t;
                                    } else if (t instanceof RuntimeMBeanException) {
                                        response = (Exception) t;
                                    } else {
                                        response = new IOException("Internal server error.");
                                        log.warn("Unexpected internal error", t);
                                    }

                                    sendIOException(response);
                                } else {
                                    log.error("null correlationId so error not sent to client", t);
                                }
                            } finally {
                                IoUtils.safeClose(dis);
                            }
                        }

                        private void sendIOException(final Exception e) {
                            try {
                                WriteUtil.writeExceptionResponse(channel, e, messageId, correlationId);

                                log.tracef("[%d] %h - Success Response Sent", correlationId, messageId);
                            } catch (IOException ioe) {
                                log.error(ioe);
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
                // TODO - Propper shut down logic.
                channel.receiveMessage(this);
            }
        }

        public void handleError(Channel channel, IOException error) {
            // TODO Auto-generated method stub

        }

        public void handleEnd(Channel channel) {
            // TODO Auto-generated method stub

        }

    }
}
