package org.jboss.naming.client.protocol.v1;

import java.io.Serializable;

/**
 * @author John Bailey
 */
public class ContextMarker implements Serializable {
    static final ContextMarker INSTANCE = new ContextMarker();
}
