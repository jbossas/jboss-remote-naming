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

package org.jboss.naming.remote.client;

import org.jboss.remoting3.Endpoint;
import org.xnio.OptionMap;

import javax.security.auth.callback.CallbackHandler;
import java.net.URI;

/**
 * Holds the connection information for a {@link RemoteNamingStore}
 *
 * @author Jaikiran Pai
 */
public class RemoteNamingStoreConnectionInfo {

    private final Endpoint clientEndpoint;

    private final URI connectURI;
    private final OptionMap connectOptions;
    private final long connectionTimeout;
    private final CallbackHandler callbackHandler;

    private final long channelCreationTimeoutInMillis;
    private final OptionMap channelCreationOptions;

    public RemoteNamingStoreConnectionInfo(final Endpoint clientEndpoint, final URI connectURI, final OptionMap connectionOptions,
                                           final long connectionTimeoutInMillis, final CallbackHandler callbackHandler,
                                           final long channelCreationTimeoutInMillis, final OptionMap channelCreationOptions) {
        this.clientEndpoint = clientEndpoint;
        this.connectURI = connectURI;
        this.connectOptions = connectionOptions == null ? OptionMap.EMPTY : connectionOptions;
        this.connectionTimeout = connectionTimeoutInMillis;
        this.callbackHandler = callbackHandler;
        this.channelCreationOptions = channelCreationOptions == null ? OptionMap.EMPTY : channelCreationOptions;
        this.channelCreationTimeoutInMillis = channelCreationTimeoutInMillis;
    }

    public Endpoint getEndpoint() {
        return this.clientEndpoint;
    }

    public URI getConnectionURI() {
        return this.connectURI;
    }

    public OptionMap getConnectionOptions() {
        return this.connectOptions;
    }

    public long getConnectionTimeout() {
        return this.connectionTimeout;
    }

    public CallbackHandler getCallbackHandler() {
        return this.callbackHandler;
    }

    public OptionMap getChannelCreationOptions() {
        return this.channelCreationOptions;
    }

    public long getChannelCreationTimeout() {
        return this.channelCreationTimeoutInMillis;
    }
}
