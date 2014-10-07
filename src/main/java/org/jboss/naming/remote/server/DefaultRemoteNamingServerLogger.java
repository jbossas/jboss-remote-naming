package org.jboss.naming.remote.server;

import java.io.IOException;
import org.jboss.logging.Logger;
import org.jboss.remoting3.Channel;

/**
 * @author John Bailey
 */
public class DefaultRemoteNamingServerLogger implements RemoteNamingServerLogger{
    private static final Logger log = Logger.getLogger(RemoteNamingService.class.getPackage().getName());

    public static DefaultRemoteNamingServerLogger INSTANCE = new DefaultRemoteNamingServerLogger();

    private DefaultRemoteNamingServerLogger() {
    }

    public void failedToSendHeader(final IOException exception) {
        log.error("Unable to send header, closing channel", exception);
    }

    public void failedToDetermineClientVersion(final IOException exception) {
        log.error("Error determining version selected by client.", exception);
    }

    public void closingChannel(final Channel channel, final Throwable error) {
        log.errorf(error, "Closing channel %s due to an error", channel);
    }

    public void closingChannelOnChannelEnd(final Channel channel) {
        log.debugf("Channel end notification received, closing channel %s", channel);
    }

    public void unnexpectedError(final Throwable t) {
        log.warn("Unexpected internal error", t);
    }

    public void nullCorrelationId(final Throwable t) {
        log.error("null correlationId so error not sent to client", t);
    }

    public void failedToSendExceptionResponse(final IOException ioe) {
        log.error("Failed to send exception response to client", ioe);
    }

    public void unexpectedParameterType(byte expected, byte actual) {
        log.errorf("Unexpected parameter type - excpected: %d  received: %d", expected, actual);
    }
}
