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

package org.jboss.naming.remote.client.ejb;

import org.jboss.ejb.client.ContextSelector;
import org.jboss.ejb.client.EJBClient;
import org.jboss.ejb.client.EJBClientContext;
import org.jboss.ejb.client.EJBClientContextIdentifier;
import org.jboss.ejb.client.EJBLocator;
import org.jboss.ejb.client.IdentityEJBClientContextSelector;
import org.jboss.ejb.client.NamedEJBClientContextIdentifier;
import org.jboss.logging.Logger;
import org.jboss.naming.remote.client.RemoteContext;
import org.jboss.remoting3.Connection;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An implementation of {@link EJBClientHandler} which has hard dependency on the EJB client APIs
 *
 * @author Jaikiran Pai
 */
public class RemoteNamingStoreEJBClientHandler implements EJBClientHandler {

    private static final Logger logger = Logger.getLogger(RemoteNamingStoreEJBClientHandler.class);

    private static final String EJB_CLIENT_CONTEXT_NAME_PREFIX = "RemoteNamingEJBClientContext$";
    private static final AtomicLong nextEJBClientContextNumber = new AtomicLong();

    private final EJBClientContextIdentifier ejbClientContextIdentifier;
    private final EJBClientContext ejbClientContext;

    // This method is referenced via reflection in the org.jboss.naming.remote.client.InitialContextFactory
    // to avoid any hard dependencies between the core remote naming APIs and the EJB client APIs. That way,
    // remote naming is still functional even in the absence of EJB client API jars.
    public static RemoteNamingStoreEJBClientHandler setupEJBClientContext(final List<RemoteContext.CloseTask> closeTasks) {
        final EJBClientContext ejbClientContext = EJBClientContext.create();
        final String ejbClientContextName = EJB_CLIENT_CONTEXT_NAME_PREFIX + nextEJBClientContextNumber.addAndGet(1);
        final EJBClientContextIdentifier ejbClientContextIdentifier = new NamedEJBClientContextIdentifier(ejbClientContextName);
        // register the context with the selector
        registerEJBClientContextWithSelector(ejbClientContextIdentifier, ejbClientContext);
        // add a close task which closes the EJB client context when the remote naming context is closed
        if (closeTasks != null) {
            closeTasks.add(new RemoteNamingEJBClientContextCloseTask(ejbClientContext));
        }
        return new RemoteNamingStoreEJBClientHandler(ejbClientContextIdentifier, ejbClientContext);
    }

    private RemoteNamingStoreEJBClientHandler(final EJBClientContextIdentifier ejbClientContextIdentifier, final EJBClientContext ejbClientContext) {
        this.ejbClientContext = ejbClientContext;
        this.ejbClientContextIdentifier = ejbClientContextIdentifier;
    }

    @Override
    public void associate(final Connection connection) {
        this.ejbClientContext.registerConnection(connection);
    }

    @Override
    public Object handleLookupReturnInstance(Object instance) {
        if (instance == null) {
            return null;
        }
        if (!EJBClient.isEJBProxy(instance)) {
            return instance;
        }
        final EJBLocator ejbLocator = EJBClient.getLocatorFor(instance);
        // recreate the proxy by associating it with the EJB client context identifier applicable for this
        // remote naming context
        return EJBClient.createProxy(ejbLocator, this.ejbClientContextIdentifier);
    }

    private static void registerEJBClientContextWithSelector(final EJBClientContextIdentifier identifier, final EJBClientContext ejbClientContext) {
        final ContextSelector<EJBClientContext> currentSelector = EJBClientContext.getSelector();
        // if the selector isn't able to handle identity based EJB client contexts, then we don't create one.
        if (!(currentSelector instanceof IdentityEJBClientContextSelector)) {
            logger.info("Cannot create a scoped EJB client context for JNDI naming context since the current " +
                    "EJB client context selector can't handle scoped contexts");
        } else {
            // register it with the identity based EJB client context selector
            ((IdentityEJBClientContextSelector) currentSelector).registerContext(identifier, ejbClientContext);
        }
    }

    private static class RemoteNamingEJBClientContextCloseTask implements RemoteContext.CloseTask {

        private EJBClientContext ejbClientContext;

        private RemoteNamingEJBClientContextCloseTask(final EJBClientContext clientContext) {
            this.ejbClientContext = clientContext;
        }

        @Override
        public void close(boolean isFinalize) {
            try {
                this.ejbClientContext.close();
            } catch (IOException e) {
                logger.debug("Failed to close EJB client context " + this.ejbClientContext, e);
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if(!(obj instanceof RemoteNamingStoreEJBClientHandler)) {
            return false;
        }
        RemoteNamingStoreEJBClientHandler that = (RemoteNamingStoreEJBClientHandler) obj;
        return this.ejbClientContextIdentifier.equals(that.ejbClientContextIdentifier) && this.ejbClientContext.equals(that.ejbClientContext);
    }
}
