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

package org.jboss.naming.remote.client;

import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.naming.Binding;
import javax.naming.CompositeName;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import org.jboss.logging.Logger;
import static org.jboss.naming.remote.client.ClientUtil.isEmpty;
import static org.jboss.naming.remote.client.ClientUtil.namingEnumeration;

/**
 * @author John Bailey
 */
public class RemoteContext implements Context, NameParser {
    private static final Logger log = Logger.getLogger(RemoteContext.class);

    private final Name prefix;
    private final Hashtable<String, Object> environment;
    private final RemoteNamingStore namingStore;
    private final List<CloseTask> closeTasks;

    private final AtomicBoolean closed = new AtomicBoolean();

    public RemoteContext(final RemoteNamingStore namingStore, final Hashtable<String, Object> environment) {
        this(namingStore, environment, Collections.<CloseTask>emptyList());
    }

    public RemoteContext(final RemoteNamingStore namingStore, final Hashtable<String, Object> environment, final List<CloseTask> closeTasks) {
        this(new CompositeName(), namingStore, environment, closeTasks);
    }

    public RemoteContext(final Name prefix, final RemoteNamingStore namingStore, final Hashtable<String, Object> environment) {
        this(prefix, namingStore, environment, Collections.<CloseTask>emptyList());
    }

    public RemoteContext(final Name prefix, final RemoteNamingStore namingStore, final Hashtable<String, Object> environment, final List<CloseTask> closeTasks) {
        this.prefix = prefix;
        this.namingStore = namingStore;
        this.environment = environment;
        this.closeTasks = closeTasks;
    }

    public Object lookup(final Name name) throws NamingException {
        if (isEmpty(name)) {
            return new RemoteContext(prefix, namingStore, environment);
        }
        return namingStore.lookup(getAbsoluteName(name));
    }

    public Object lookup(final String name) throws NamingException {
        return lookup(parse(name));
    }

    public void bind(final Name name, final Object object) throws NamingException {
        namingStore.bind(getAbsoluteName(name), object);
    }

    public void bind(final String name, final Object object) throws NamingException {
        bind(parse(name), object);
    }

    public void rebind(final Name name, final Object object) throws NamingException {
        namingStore.rebind(name, object);
    }

    public void rebind(final String name, final Object object) throws NamingException {
        rebind(parse(name), object);
    }

    public void unbind(final Name name) throws NamingException {
        namingStore.unbind(name);
    }

    public void unbind(final String name) throws NamingException {
        unbind(parse(name));
    }

    public void rename(final Name name, final Name newName) throws NamingException {
        namingStore.rename(name, newName);
    }

    public void rename(final String name, final String newName) throws NamingException {
        rename(parse(name), parse(newName));
    }

    public NamingEnumeration<NameClassPair> list(final Name name) throws NamingException {
        return namingEnumeration(namingStore.list(name));
    }

    public NamingEnumeration<NameClassPair> list(final String name) throws NamingException {
        return list(parse(name));
    }

    public NamingEnumeration<Binding> listBindings(final Name name) throws NamingException {
        return namingEnumeration(namingStore.listBindings(name));
    }

    public NamingEnumeration<Binding> listBindings(final String name) throws NamingException {
        return listBindings(parse(name));
    }

    public void destroySubcontext(final Name name) throws NamingException {
        namingStore.destroySubcontext(name);
    }

    public void destroySubcontext(final String name) throws NamingException {
        destroySubcontext(parse(name));
    }

    public Context createSubcontext(final Name name) throws NamingException {
        return namingStore.createSubcontext(name);
    }

    public Context createSubcontext(final String name) throws NamingException {
        return createSubcontext(parse(name));
    }

    public Object lookupLink(final Name name) throws NamingException {
        return namingStore.lookupLink(name);
    }

    public Object lookupLink(final String name) throws NamingException {
        return lookupLink(parse(name));
    }

    public NameParser getNameParser(Name name) throws NamingException {
        return this;
    }

    public NameParser getNameParser(String s) throws NamingException {
        return this;
    }

    public Name composeName(Name name, Name prefix) throws NamingException {
        final Name result = (Name) prefix.clone();
        result.addAll(name);
        return result;
    }

    public String composeName(String name, String prefix) throws NamingException {
        return composeName(parse(name), parse(prefix)).toString();
    }

    public Object addToEnvironment(String s, Object o) throws NamingException {
        return environment.put(s, o);
    }

    public Object removeFromEnvironment(String s) throws NamingException {
        return environment.remove(s);
    }

    public Hashtable<?, ?> getEnvironment() throws NamingException {
        return environment;
    }

    public void close() throws NamingException {
        if(closed.compareAndSet(false, true)) {
            for (CloseTask closeTask : closeTasks) {
                closeTask.close(false);
            }
        }
    }

    public void finalize() {
        if(closed.compareAndSet(false, true)) {
            for (CloseTask closeTask : closeTasks) {
                closeTask.close(true);
            }
        }
    }

    public String getNameInNamespace() throws NamingException {
        return prefix.toString();
    }

    public Name parse(final String name) throws NamingException {
        return new CompositeName(name);
    }

    private Name getAbsoluteName(final Name name) throws NamingException {
        if (name.isEmpty()) {
            return composeName(name, prefix);
        }
        return composeName(name, prefix);
    }

    static interface CloseTask {
        void close(boolean isFinalize);
    }
}
