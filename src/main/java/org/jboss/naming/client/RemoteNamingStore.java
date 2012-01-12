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

package org.jboss.naming.client;

import java.util.List;
import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NamingException;

/**
 * @author John Bailey
 */
public interface RemoteNamingStore {
    Object lookup(Name name) throws NamingException;
    void bind(final Name name, final Object object) throws NamingException;
    void rebind(Name name, Object object) throws NamingException;
    void rename(Name name, Name object) throws NamingException;
    List<NameClassPair> list(Name name) throws NamingException;
    List<Binding> listBindings(Name name) throws NamingException;
    void unbind(Name name) throws NamingException;
    Context createSubcontext(Name name) throws NamingException;
    void destroySubcontext(Name name) throws NamingException;
    Object lookupLink(final Name name) throws NamingException;
    void close() throws NamingException;
}
