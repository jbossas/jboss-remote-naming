package org.jboss.naming.client.protocol;

import java.io.DataInput;
import java.io.IOException;
import javax.naming.NamingException;
import org.jboss.naming.client.RemoteNamingService;
import org.jboss.naming.client.RemoteNamingStore;
import org.jboss.remoting3.Channel;

/**
 * @author John Bailey
 */
public interface ProtocolCommand<T> {
    byte getCommandId();

    T execute(Channel channel, Object... args) throws IOException, NamingException;

    void handleServerMessage(Channel channel, DataInput input, int correlationId, RemoteNamingService namingServer) throws IOException;

    void handleClientMessage(DataInput input, int correlationId, RemoteNamingStore namingStore) throws IOException;
}
