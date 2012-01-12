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
package org.jboss.naming.client.protocol;

import java.io.IOException;
import org.jboss.naming.client.RemoteNamingService;
import org.jboss.naming.client.RemoteNamingStore;
import org.jboss.naming.client.RemoteNamingServer;
import org.jboss.naming.client.protocol.v1.VersionOne;
import org.jboss.remoting3.Channel;

/**
 * Single access point to locate the supported versions.
 *
 * @author John Bailey
 * @author <a href="mailto:darran.lofthouse@jboss.com">Darran Lofthouse</a>
 */
public class Versions {

    private Versions() {
    }

    public static byte[] getSupportedVersions() {
        // At a later point a more complex registry or discovery could be implemented.
        return new byte[] { VersionOne.getVersionIdentifier() };
    }

    public static RemoteNamingStore getRemoteNamingStore(final byte version, final Channel channel) throws IOException {
        if (version == VersionOne.getVersionIdentifier()) {
            return VersionOne.getRemoteNamingStore(channel);
        }

        throw new IllegalArgumentException("Unsupported protocol version [" + version + "]");
    }

    public static RemoteNamingServer getRemoteNamingServer(final byte version, final Channel channel, final RemoteNamingService remoteNamingServer) {
        if (version == VersionOne.getVersionIdentifier()) {
            return VersionOne.getNamingServer(channel, remoteNamingServer);
        }
        throw new IllegalArgumentException("Unsupported protocol version [" + version + "]");
    }
}
