package org.jboss.naming.client;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.naming.Binding;
import javax.naming.CompositeName;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameAlreadyBoundException;
import javax.naming.NameClassPair;
import javax.naming.NameNotFoundException;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.spi.NamingManager;
import static org.jboss.naming.client.ClientUtil.namingEnumeration;
import static org.jboss.naming.client.ClientUtil.namingException;

/**
 * @author John Bailey
 */
public class MockContext implements Context {
    private ConcurrentSkipListMap<Name, Binding> bindings = new ConcurrentSkipListMap<Name, Binding>();

    public Object lookup(final Name name) throws NamingException {
        if (name.size() == 0) {
            return this;
        }
        final Name lower = bindings.lowerKey(name);
        if (lower != null) {
            Binding lowerBinding = bindings.get(lower);
            if (lowerBinding.getClassName().equals(Context.class.getName())) {
                final Name childName = name.getSuffix(lower.size());
                return Context.class.cast(lowerBinding.getObject()).lookup(childName);
            }
        }
        if (bindings.containsKey(name)) {
            return bindings.get(name).getObject();
        }
        throw new NameNotFoundException(name.toString());
    }

    public Object lookup(String s) throws NamingException {
        return lookup(new CompositeName(s));
    }

    public void bind(Name name, Object object) throws NamingException {
        bind(name, object.getClass(), object);
    }

    public void bind(String s, Object o) throws NamingException {
        bind(new CompositeName(s), o);
    }

    protected void bind(final Name name, final Class<?> type, final Object object) throws NamingException {
        if (name.size() > 1) {
            final Name contextName = name.getPrefix(name.size() - 1);
            if (contextName.size() > 0) {
                final Context context = (Context) lookup(contextName);
                context.bind(name.getSuffix(name.size() - 1), object);
                return;
            }
        }
        if (bindings.containsKey(name)) {
            throw new NameAlreadyBoundException(name.toString());
        }
        bindings.put(name, new Binding(name.toString(), type.getName(), object, true));
    }

    public void rebind(final Name name, final Object o) throws NamingException {
        unbind(name);
        bind(name, o);
    }

    public void rebind(final String s, final Object o) throws NamingException {
        rebind(new CompositeName(s), o);
    }

    public void unbind(final Name name) throws NamingException {
        if (name.size() > 1) {
            final Name contextName = name.getPrefix(name.size() - 1);
            if (contextName.size() > 0) {
                final Context context = (Context) lookup(contextName);
                context.unbind(name.getSuffix(name.size() - 1));
                return;
            }
        }
        if (!bindings.containsKey(name)) {
            throw new NameNotFoundException(name.toString());
        }
        bindings.remove(name);
    }

    public void unbind(String s) throws NamingException {
        unbind(new CompositeName(s));
    }

    public void rename(final Name name, final Name newName) throws NamingException {
        final Object value = lookup(name);
        unbind(name);
        bind(newName, value);
    }

    public void rename(final String s, final String s1) throws NamingException {
        rename(new CompositeName(s), new CompositeName(s1));
    }

    public NamingEnumeration<NameClassPair> list(final Name name) throws NamingException {
        if (name.size() > 0) {
            final Name contextName = name.getPrefix(name.size() - 1);
            final Context context = (Context) lookup(contextName);
            return context.list("");
        }

        final List<NameClassPair> results = new ArrayList<NameClassPair>();
        for (Map.Entry<Name, Binding> entry : bindings.entrySet()) {
            results.add(new NameClassPair(entry.getKey().toString(), entry.getValue().getClassName()));
        }
        return namingEnumeration(results);
    }

    public NamingEnumeration<NameClassPair> list(String s) throws NamingException {
        return list(new CompositeName(s));
    }

    public NamingEnumeration<Binding> listBindings(Name name) throws NamingException {
        if (name.size() > 0) {
            final Name contextName = name.getPrefix(name.size() - 1);
            final Context context = (Context) lookup(contextName);
            return context.listBindings("");
        }

        final List<Binding> results = new ArrayList<Binding>();
        for (Map.Entry<Name, Binding> entry : bindings.entrySet()) {
            if (entry.getKey().size() == 1) {
                results.add(entry.getValue());
            }
        }
        return namingEnumeration(results);
    }

    public NamingEnumeration<Binding> listBindings(String s) throws NamingException {
        return listBindings(new CompositeName(s));
    }

    public void destroySubcontext(Name name) throws NamingException {
        unbind(name);
    }

    public void destroySubcontext(String s) throws NamingException {
        destroySubcontext(new CompositeName(s));
    }

    public Context createSubcontext(final Name name) throws NamingException {
        final MockContext context = new MockContext();
        bind(name, Context.class, context);
        return context;
    }

    public Context createSubcontext(String s) throws NamingException {
        return createSubcontext(new CompositeName(s));
    }

    public Object lookupLink(final Name name) throws NamingException {
        return lookup(name);
    }

    public Object lookupLink(String s) throws NamingException {
        return lookupLink(new CompositeName(s));
    }

    public NameParser getNameParser(Name name) throws NamingException {
        return null;
    }

    public NameParser getNameParser(String s) throws NamingException {
        return null;
    }

    public Name composeName(Name name, Name name1) throws NamingException {
        return null;
    }

    public String composeName(String s, String s1) throws NamingException {
        return null;
    }

    public Object addToEnvironment(String s, Object o) throws NamingException {
        return null;
    }

    public Object removeFromEnvironment(String s) throws NamingException {
        return null;
    }

    public Hashtable<?, ?> getEnvironment() throws NamingException {
        return null;
    }

    public void close() throws NamingException {

    }

    public String getNameInNamespace() throws NamingException {
        return null;
    }
}
