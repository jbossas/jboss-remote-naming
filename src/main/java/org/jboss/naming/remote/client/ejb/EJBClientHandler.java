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

import org.jboss.remoting3.Connection;

/**
 * A {@link EJBClientHandler} is an abstraction to allows the remote naming APIs to support EJB invocations
 * without adding a hard dependency on the EJB client project.
 * <p/>
 * This {@link EJBClientHandler} interface is expected to have no references to any of the EJB client APIs.
 * The implementations of this interface can however refer to the EJB client APIs.
 *
 * @author Jaikiran Pai
 */
public interface EJBClientHandler {

    /**
     * Associates the passed <code>connection</code> with an appropriate {@link org.jboss.ejb.client.EJBClientContext}.
     * The passed <code>connection</code> is typically managed by the remote naming APIs
     *
     * @param connection The connection to be associated with the EJB client context
     * @throws Exception
     */
    void associate(final Connection connection) throws Exception;

    /**
     * This method will be invoked by the remote naming lookup protocol after it has received back the object
     * from the server, during a {@link javax.naming.Context#lookup(String)} or {@link javax.naming.Context#lookup(javax.naming.Name)}
     * operation. This allows the {@link EJBClientHandler} implementations to check if the returned object is an
     * {@link org.jboss.ejb.client.EJBClient#isEJBProxy(Object) EJB proxy} and if it is, then do any relevant processing
     * of that proxy before returning back the processed/updated proxy.
     * <p/>
     * If the passed <code>instance</code> is not an EJB proxy then the implementations of this {@link EJBClientHandler}
     * are expected to just return back the passed <code>instance</code>.
     *
     * @param instance The object instance which was returned by the server after a remote naming lookup
     * @return
     */
    Object handleLookupReturnInstance(Object instance);
}
