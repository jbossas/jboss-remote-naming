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

import java.util.Collection;
import java.util.Iterator;
import javax.naming.Name;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;

/**
 * @author John Bailey
 */
public class ClientUtil {
    public static NamingException namingException(final String message, final Throwable cause) {
        final NamingException namingException = new NamingException(message);
        namingException.setRootCause(cause);
        return namingException;
    }

    public static boolean isEmpty(final Name name) {
        return name.isEmpty() || (name.size() == 1 && "".equals(name.get(0)));
    }

    public static <T> NamingEnumeration<T> namingEnumeration(final Collection<T> collection) {
        final Iterator<T> iterator = collection.iterator();
        return new NamingEnumeration<T>() {
            public T next() {
                return nextElement();
            }

            public boolean hasMore() {
                return hasMoreElements();
            }

            public void close() {
            }

            public boolean hasMoreElements() {
                return iterator.hasNext();
            }

            public T nextElement() {
                return iterator.next();
            }
        };
    }
}
