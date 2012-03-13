package org.jboss.naming.remote.protocol;

import javax.naming.NamingException;

/**
 * Exception class that signifies that the naming operation failed due to an {@link java.io.IOException}
 *
 * @author Stuart Douglas
 */
public class NamingIOException extends NamingException {

    public NamingIOException(final String explanation) {
        super(explanation);
    }

    public NamingIOException() {
    }
}
