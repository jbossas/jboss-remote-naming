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
import org.jboss.naming.remote.protocol.ProtocolCommand;
import org.jboss.naming.remote.server.RemoteNamingServer;
import org.jboss.naming.remote.server.RemoteNamingServerLogger;
import org.jboss.naming.remote.server.RemoteNamingService;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.MessageInputStream;
import org.xnio.IoUtils;

import static org.jboss.naming.remote.protocol.v1.WriteUtil.writeExceptionResponse;

/**
 * @author John Bailey
 */
public class RemoteNamingServerV1 implements RemoteNamingServer {
    private static final Logger log = Logger.getLogger(RemoteNamingServerV1.class);

    private final Channel channel;
    private final RemoteNamingService remoteNamingService;
    private final RemoteNamingServerLogger logger;

    public RemoteNamingServerV1(final Channel channel, final RemoteNamingService remoteNamingServer) {
        this.channel = channel;
        this.remoteNamingService = remoteNamingServer;
        this.logger = remoteNamingServer.getLogger();
    }

    public void start() {
        channel.receiveMessage(new MessageReciever());
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
                    remoteNamingService.getExecutor().execute(new Runnable() {
                        public void run() {
                            try {
                                command.handleServerMessage(channel, dis, correlationId, remoteNamingService);
                            } catch (Throwable t) {
                                if (correlationId != 0x00) {
                                    Exception response;
                                    if (t instanceof IOException) {
                                        response = (Exception) t;
                                    } else if (t instanceof RuntimeMBeanException) {
                                        response = (Exception) t;
                                    } else {
                                        response = new IOException("Internal server error.");
                                        logger.unnexpectedError(t);
                                    }

                                    sendIOException(response);
                                } else {
                                    logger.nullCorrelationId(t);
                                }
                            } finally {
                                IoUtils.safeClose(dis);
                            }
                        }

                        private void sendIOException(final Exception e) {
                            try {
                                writeExceptionResponse(channel, e, messageId, correlationId);
                                log.tracef("[%d] %h - Success Response Sent", correlationId, messageId);
                            } catch (IOException ioe) {
                                logger.failedToSendExceptionResponse(ioe);
                            }
                        }

                    });

                } else {
                    throw new IOException("Unrecognised Message ID");
                }
            } catch (Throwable e) {
                logger.unnexpectedError(e);
                IoUtils.safeClose(dis);
            } finally {
                channel.receiveMessage(this);
            }
        }

        public void handleError(final Channel channel, final IOException error) {
            logger.closingChannel(channel, error);
            try {
                channel.close();
            } catch (IOException ignore) {
            }
        }

        public void handleEnd(final Channel channel) {
            logger.closingChannelOnChannelEnd(channel);
            try {
                channel.close();
            } catch (IOException ignore) {
            }
        }
    }
}
