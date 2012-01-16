package org.jboss.naming.remote.server;

import java.io.IOException;
import org.jboss.remoting3.Channel;

/**
 * @author John Bailey
 */
public interface RemoteNamingServerLogger {
    void failedToSendHeader(final IOException exception);
    void failedToDetermineClientVersion(final IOException exception);
    void closingChannel(Channel channel, Throwable t);
    void closingChannelOnChannelEnd(Channel channel);

    void unnexpectedError(Throwable t);

    void nullCorrelationId(Throwable t);

    void failedToSendExceptionResponse(IOException ioe);

    void unexpectedParameterType(byte expected, byte actual);
}
