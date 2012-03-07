/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
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
package org.jboss.naming.remote.protocol.v1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.InvalidNameException;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;

import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Unmarshaller;
import org.jboss.naming.remote.client.RemoteContext;
import org.jboss.naming.remote.client.RemoteNamingStore;
import org.jboss.naming.remote.protocol.ProtocolCommand;
import org.jboss.naming.remote.server.RemoteNamingService;
import org.jboss.remoting3.Channel;
import org.xnio.IoFuture;

import static org.jboss.naming.remote.client.ClientUtil.namingException;
import static org.jboss.naming.remote.protocol.v1.Constants.BINDING;
import static org.jboss.naming.remote.protocol.v1.Constants.CONTEXT;
import static org.jboss.naming.remote.protocol.v1.Constants.NAME;
import static org.jboss.naming.remote.protocol.v1.Constants.OBJECT;
import static org.jboss.naming.remote.protocol.v1.Constants.SUCCESS;
import static org.jboss.naming.remote.protocol.v1.ReadUtil.prepareForUnMarshalling;
import static org.jboss.naming.remote.protocol.v1.WriteUtil.prepareForMarshalling;
import static org.jboss.naming.remote.protocol.v1.WriteUtil.write;
import static org.jboss.naming.remote.protocol.v1.WriteUtil.writeExceptionResponse;
import static org.jboss.naming.remote.protocol.v1.WriteUtil.writeResponse;

/**
 * @author John Bailey
 */
class Protocol {
    static ProtocolCommand<Object> LOOKUP = new BaseProtocolCommand<Object>((byte) 0x01) {
        public Object execute(final Channel channel, final Object... args) throws IOException, NamingException {
            if (args.length != 1 || !(args[0] instanceof Name)) {
                throw new IllegalArgumentException("Lookup requires a single name argument");
            }
            final Name name = Name.class.cast(args[0]);
            final NamedIoFuture<Object> future = new NamedIoFuture<Object>(name);
            final int correlationId = reserveNextCorrelationId(future);
            try {
                write(channel, new WriteUtil.Writer() {
                    public void write(final DataOutput output) throws IOException {
                        output.writeByte(getCommandId());
                        output.writeInt(correlationId);

                        final Marshaller marshaller = prepareForMarshalling(output);
                        marshaller.writeByte(NAME);
                        marshaller.writeObject(name);
                        marshaller.finish();
                    }
                });

                final IoFuture.Status result = future.await(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                switch (result) {
                    case FAILED:
                        if (future.getHeldException() != null) {
                            throw future.getHeldException();
                        }
                        throw future.getException();
                    case DONE:
                        return future.get();
                    default:
                        throw new NamingException("Unable to invoke lookup, status=" + result.toString());
                }

            } catch (NamingException e) {
                throw e;
            } catch (Exception e) {
                throw namingException("Failed to lookup", e);
            } finally {
                releaseCorrelationId(correlationId);
            }
        }

        public void handleServerMessage(Channel channel, final DataInput input, final int correlationId, final RemoteNamingService remoteNamingService) throws IOException {

            final Unmarshaller unmarshaller = prepareForUnMarshalling(input);
            Name name;
            try {
                byte paramType = unmarshaller.readByte();
                if (paramType != NAME) {
                    remoteNamingService.getLogger().unexpectedParameterType(NAME, paramType);
                }
                name = unmarshaller.readObject(Name.class);
            } catch (ClassNotFoundException cnfe) {
                throw new IOException(cnfe);
            } finally {
                unmarshaller.close();
            }

            try {
                final Object result = remoteNamingService.getLocalContext().lookup(name);
                write(channel, new WriteUtil.Writer() {
                    public void write(DataOutput output) throws IOException {
                        output.writeByte(getCommandId());
                        output.writeInt(correlationId);
                        output.writeByte(SUCCESS);
                        if (result instanceof Context) {
                            output.writeByte(CONTEXT);
                        } else {
                            output.writeByte(OBJECT);
                            final Marshaller marshaller = prepareForMarshalling(output);
                            marshaller.writeObject(result);
                            marshaller.finish();
                        }
                    }
                });
            } catch (NamingException e) {
                writeExceptionResponse(channel, e, getCommandId(), correlationId);
            }
        }

        public void handleClientMessage(final DataInput input, final int correlationId, final RemoteNamingStore namingStore) throws IOException {
            readResult(correlationId, input, new ValueReader<Object>() {
                public void read(final DataInput input, ProtocolIoFuture<Object> future) throws IOException {
                    byte parameterType = input.readByte();
                    switch (parameterType) {
                        case OBJECT: {
                            try {
                                final Unmarshaller unmarshaller = prepareForUnMarshalling(input);
                                future.setResult(unmarshaller.readObject());
                                unmarshaller.finish();
                            } catch (ClassNotFoundException e) {
                                throw new IOException(e);
                            } catch (ClassCastException e) {
                                throw new IOException(e);
                            }
                            break;
                        }
                        case CONTEXT: {
                            future.setResult(new RemoteContext(NamedIoFuture.class.cast(future).name, namingStore, new Hashtable<String, Object>()));
                            break;
                        }
                        default: {
                            throw new IOException("Unexpected response parameter received.");
                        }

                    }
                }
            });
        }
    };

    static ProtocolCommand<Void> BIND = new BaseProtocolCommand<Void>((byte) 0x02) {
        public Void execute(final Channel channel, final Object... args) throws IOException, NamingException {
            if (args.length != 2 || !(args[0] instanceof Name)) {
                throw new IllegalArgumentException("Rebind requires a name and object argument");
            }
            final Name name = Name.class.cast(args[0]);
            final Object object = args[1];

            final ProtocolIoFuture<Void> future = new ProtocolIoFuture<Void>();
            final int correlationId = reserveNextCorrelationId(future);
            try {
                write(channel, new WriteUtil.Writer() {
                    public void write(final DataOutput output) throws IOException {
                        output.writeByte(getCommandId());
                        output.writeInt(correlationId);

                        final Marshaller marshaller = prepareForMarshalling(output);
                        marshaller.writeByte(NAME);
                        marshaller.writeObject(name);
                        marshaller.writeByte(OBJECT);
                        marshaller.writeObject(object);
                        marshaller.finish();
                    }
                });

                final IoFuture.Status result = future.await(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                switch (result) {
                    case FAILED:
                        if (future.getHeldException() != null) {
                            throw future.getHeldException();
                        }
                        throw future.getException();
                    case DONE:
                        return null;
                    default:
                        throw new NamingException("Unable to invoke bind, status=" + result.toString());
                }

            } catch (NamingException e) {
                throw e;
            } catch (Exception e) {
                throw namingException("Failed to bind", e);
            } finally {
                releaseCorrelationId(correlationId);
            }
        }

        public void handleServerMessage(final Channel channel, final DataInput input, final int correlationId, final RemoteNamingService remoteNamingService) throws IOException {
            final Unmarshaller unmarshaller = prepareForUnMarshalling(input);
            Name name;
            Object object;
            try {
                byte paramType = unmarshaller.readByte();
                if (paramType != NAME) {
                    remoteNamingService.getLogger().unexpectedParameterType(NAME, paramType);
                }
                name = unmarshaller.readObject(Name.class);

                paramType = unmarshaller.readByte();
                if (paramType != OBJECT) {
                    remoteNamingService.getLogger().unexpectedParameterType(OBJECT, paramType);
                }
                object = unmarshaller.readObject();
            } catch (ClassNotFoundException cnfe) {
                throw new IOException(cnfe);
            } finally {
                unmarshaller.finish();
            }

            try {
                remoteNamingService.getLocalContext().bind(name, object);
                writeResponse(channel, getCommandId(), correlationId);
            } catch (NamingException e) {
                writeExceptionResponse(channel, e, getCommandId(), correlationId);
            }
        }

        public void handleClientMessage(DataInput input, int correlationId, RemoteNamingStore namingStore) throws IOException {
            readResult(correlationId, input, new ValueReader<Object>() {
                public void read(final DataInput input, ProtocolIoFuture<Object> future) throws IOException {
                    future.setResult(null);
                }
            });
        }
    };

    static ProtocolCommand<Void> REBIND = new BaseProtocolCommand<Void>((byte) 0x03) {
        public Void execute(final Channel channel, final Object... args) throws IOException, NamingException {
            if (args.length != 2 || !(args[0] instanceof Name)) {
                throw new IllegalArgumentException("Bind requires a name and object argument");
            }
            final Name name = Name.class.cast(args[0]);
            final Object object = args[1];

            final ProtocolIoFuture<Void> future = new ProtocolIoFuture<Void>();
            final int correlationId = reserveNextCorrelationId(future);
            try {
                write(channel, new WriteUtil.Writer() {
                    public void write(final DataOutput output) throws IOException {
                        output.writeByte(getCommandId());
                        output.writeInt(correlationId);

                        final Marshaller marshaller = prepareForMarshalling(output);
                        marshaller.writeByte(NAME);
                        marshaller.writeObject(name);
                        marshaller.writeByte(OBJECT);
                        marshaller.writeObject(object);
                        marshaller.finish();
                    }
                });

                final IoFuture.Status result = future.await(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                switch (result) {
                    case FAILED:
                        if (future.getHeldException() != null) {
                            throw future.getHeldException();
                        }
                        throw future.getException();
                    case DONE:
                        return null;
                    default:
                        throw new NamingException("Unable to invoke rebind, status=" + result.toString());
                }

            } catch (NamingException e) {
                throw e;
            } catch (Exception e) {
                throw namingException("Failed to rebind", e);
            } finally {
                releaseCorrelationId(correlationId);
            }
        }

        public void handleServerMessage(final Channel channel, final DataInput input, final int correlationId, final RemoteNamingService remoteNamingService) throws IOException {
            final Unmarshaller unmarshaller = prepareForUnMarshalling(input);
            Name name;
            Object object;
            try {
                byte paramType = unmarshaller.readByte();
                if (paramType != NAME) {
                    remoteNamingService.getLogger().unexpectedParameterType(NAME, paramType);
                }
                name = unmarshaller.readObject(Name.class);

                paramType = unmarshaller.readByte();
                if (paramType != OBJECT) {
                    remoteNamingService.getLogger().unexpectedParameterType(OBJECT, paramType);
                }
                object = unmarshaller.readObject();
            } catch (ClassNotFoundException cnfe) {
                throw new IOException(cnfe);
            } finally {
                unmarshaller.finish();
            }

            try {
                remoteNamingService.getLocalContext().rebind(name, object);
                writeResponse(channel, getCommandId(), correlationId);
            } catch (NamingException e) {
                writeExceptionResponse(channel, e, getCommandId(), correlationId);
            }
        }

        public void handleClientMessage(DataInput input, int correlationId, RemoteNamingStore namingStore) throws IOException {
            readResult(correlationId, input, new ValueReader<Object>() {
                public void read(final DataInput input, ProtocolIoFuture<Object> future) throws IOException {
                    future.setResult(null);
                }
            });
        }
    };

    static ProtocolCommand<List<NameClassPair>> LIST = new BaseProtocolCommand<List<NameClassPair>>((byte) 0x04) {
        public List<NameClassPair> execute(final Channel channel, final Object... args) throws IOException, NamingException {
            if (args.length != 1 || !(args[0] instanceof Name)) {
                throw new IllegalArgumentException("List requires a name argument.");
            }
            final Name name = Name.class.cast(args[0]);

            final ProtocolIoFuture<List<NameClassPair>> future = new ProtocolIoFuture<List<NameClassPair>>();
            final int correlationId = reserveNextCorrelationId(future);
            try {
                write(channel, new WriteUtil.Writer() {
                    public void write(final DataOutput output) throws IOException {
                        output.writeByte(getCommandId());
                        output.writeInt(correlationId);

                        final Marshaller marshaller = prepareForMarshalling(output);
                        marshaller.writeByte(NAME);
                        marshaller.writeObject(name);
                        marshaller.finish();
                    }
                });

                final IoFuture.Status result = future.await(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                switch (result) {
                    case FAILED:
                        if (future.getHeldException() != null) {
                            throw future.getHeldException();
                        }
                        throw future.getException();
                    case DONE:
                        return future.get();
                    default:
                        throw new NamingException("Unable to invoke list, status=" + result.toString());
                }

            } catch (NamingException e) {
                throw e;
            } catch (Exception e) {
                throw namingException("Failed to list", e);
            } finally {
                releaseCorrelationId(correlationId);
            }
        }

        public void handleServerMessage(final Channel channel, final DataInput input, final int correlationId, final RemoteNamingService remoteNamingService) throws IOException {
            final Unmarshaller unmarshaller = prepareForUnMarshalling(input);
            Name name;
            try {
                byte paramType = unmarshaller.readByte();
                if (paramType != NAME) {
                    remoteNamingService.getLogger().unexpectedParameterType(NAME, paramType);
                }
                name = unmarshaller.readObject(Name.class);
            } catch (ClassNotFoundException cnfe) {
                throw new IOException(cnfe);
            } finally {
                unmarshaller.finish();
            }

            try {
                final NamingEnumeration<NameClassPair> results = remoteNamingService.getLocalContext().list(name);
                final List<NameClassPair> resultList = new ArrayList<NameClassPair>();
                while (results.hasMore()) {
                    resultList.add(results.next());
                }
                write(channel, new WriteUtil.Writer() {
                    public void write(DataOutput output) throws IOException {
                        output.writeByte(getCommandId());
                        output.writeInt(correlationId);
                        output.writeByte(SUCCESS);

                        output.writeByte(Constants.LIST);
                        output.writeInt(resultList.size());

                        final Marshaller marshaller = prepareForMarshalling(output);
                        for (NameClassPair nameClassPair : resultList) {
                            marshaller.writeObject(nameClassPair);
                        }
                        marshaller.finish();
                    }
                });
            } catch (NamingException e) {
                writeExceptionResponse(channel, e, getCommandId(), correlationId);
            }
        }

        public void handleClientMessage(DataInput input, int correlationId, RemoteNamingStore namingStore) throws IOException {
            readResult(correlationId, input, new ValueReader<Object>() {
                public void read(final DataInput input, ProtocolIoFuture<Object> future) throws IOException {
                    byte parameterType = input.readByte();
                    if (parameterType != Constants.LIST) {
                        throw new IOException("Unexpected response parameter received.");
                    }
                    final int listSize = input.readInt();
                    final List<NameClassPair> results = new ArrayList<NameClassPair>(listSize);
                    final Unmarshaller unmarshaller = prepareForUnMarshalling(input);
                    for (int i = 0; i < listSize; i++) {
                        try {
                            results.add(unmarshaller.readObject(NameClassPair.class));
                        } catch (ClassNotFoundException e) {
                            throw new IOException(e);
                        } catch (ClassCastException e) {
                            throw new IOException(e);
                        }
                    }
                    unmarshaller.finish();
                    future.setResult(results);
                }
            });
        }
    };

    static ProtocolCommand<List<Binding>> LIST_BINDINGS = new BaseProtocolCommand<List<Binding>>((byte) 0x05) {
        public List<Binding> execute(final Channel channel, final Object... args) throws IOException, NamingException {
            if (args.length != 1 || !(args[0] instanceof Name)) {
                throw new IllegalArgumentException("List requires a name argument.");
            }
            final Name name = Name.class.cast(args[0]);

            final NamedIoFuture<List<Binding>> future = new NamedIoFuture<List<Binding>>(name);
            final int correlationId = reserveNextCorrelationId(future);
            try {
                write(channel, new WriteUtil.Writer() {
                    public void write(final DataOutput output) throws IOException {
                        output.writeByte(getCommandId());
                        output.writeInt(correlationId);

                        final Marshaller marshaller = prepareForMarshalling(output);
                        marshaller.writeByte(NAME);
                        marshaller.writeObject(name);
                        marshaller.finish();
                    }
                });

                final IoFuture.Status result = future.await(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                switch (result) {
                    case FAILED:
                        if (future.getHeldException() != null) {
                            throw future.getHeldException();
                        }
                        throw future.getException();
                    case DONE:
                        return future.get();
                    default:
                        throw new NamingException("Unable to invoke listBindings, status=" + result.toString());
                }

            } catch (NamingException e) {
                throw e;
            } catch (Exception e) {
                throw namingException("Failed to list bindings", e);
            } finally {
                releaseCorrelationId(correlationId);
            }
        }

        public void handleServerMessage(final Channel channel, final DataInput input, final int correlationId, final RemoteNamingService remoteNamingService) throws IOException {
            final Unmarshaller unmarshaller = prepareForUnMarshalling(input);
            Name name;
            try {
                byte paramType = unmarshaller.readByte();
                if (paramType != NAME) {
                    remoteNamingService.getLogger().unexpectedParameterType(NAME, paramType);
                }
                name = unmarshaller.readObject(Name.class);
            } catch (ClassNotFoundException cnfe) {
                throw new IOException(cnfe);
            } finally {
                unmarshaller.finish();
            }

            try {
                final NamingEnumeration<Binding> results = remoteNamingService.getLocalContext().listBindings(name);
                final List<Binding> resultList = new ArrayList<Binding>();
                while (results.hasMore()) {
                    resultList.add(results.next());
                }
                write(channel, new WriteUtil.Writer() {
                    public void write(DataOutput output) throws IOException {
                        output.writeByte(getCommandId());
                        output.writeInt(correlationId);
                        output.writeByte(SUCCESS);

                        output.writeByte(Constants.LIST);
                        output.writeInt(resultList.size());
                        final Marshaller marshaller = prepareForMarshalling(output);
                        for (Binding binding : resultList) {
                            if (binding.getObject() instanceof Context) {
                                marshaller.writeByte(Constants.CONTEXT);
                                marshaller.writeUTF(binding.getName());
                            } else {
                                marshaller.writeByte(Constants.BINDING);
                                marshaller.writeObject(binding);
                            }
                        }
                        marshaller.finish();
                    }
                });
            } catch (NamingException e) {
                writeExceptionResponse(channel, e, getCommandId(), correlationId);
            }
        }

        public void handleClientMessage(final DataInput input, final int correlationId, final RemoteNamingStore namingStore) throws IOException {
            readResult(correlationId, input, new ValueReader<Object>() {
                public void read(final DataInput input, ProtocolIoFuture<Object> future) throws IOException {
                    byte parameterType = input.readByte();
                    if (parameterType != Constants.LIST) {
                        throw new IOException("Unexpected response parameter received.");
                    }
                    final int listSize = input.readInt();
                    final List<NameClassPair> results = new ArrayList<NameClassPair>(listSize);
                    final Unmarshaller unmarshaller = prepareForUnMarshalling(input);
                    for (int i = 0; i < listSize; i++) {
                        parameterType = unmarshaller.readByte();
                        switch (parameterType) {
                            case BINDING: {
                                try {
                                    final Binding binding = unmarshaller.readObject(Binding.class);
                                    results.add(binding);
                                } catch (ClassNotFoundException e) {
                                    throw new IOException(e);
                                } catch (ClassCastException e) {
                                    throw new IOException(e);
                                }
                                break;
                            }
                            case CONTEXT: {
                                final String bindingName = unmarshaller.readUTF();
                                final Name contextName;
                                try {
                                    contextName = Name.class.cast(NamedIoFuture.class.cast(future).name.clone()).add(bindingName);
                                } catch (InvalidNameException e) {
                                    throw new IOException(e);
                                }
                                final Context context = new RemoteContext(contextName, namingStore, new Hashtable<String, Object>());
                                results.add(new Binding(bindingName, Context.class.getName(), context));
                                break;
                            }
                            default: {
                                throw new IOException("Unexpected response parameter received.");
                            }
                        }
                    }
                    unmarshaller.finish();
                    future.setResult(results);
                }
            });
        }
    };

    static ProtocolCommand<Void> UNBIND = new BaseProtocolCommand<Void>((byte) 0x06) {
        public Void execute(final Channel channel, final Object... args) throws IOException, NamingException {
            if (args.length != 1 || !(args[0] instanceof Name)) {
                throw new IllegalArgumentException("Rebind requires a name");
            }
            final Name name = Name.class.cast(args[0]);

            final ProtocolIoFuture<Void> future = new ProtocolIoFuture<Void>();
            final int correlationId = reserveNextCorrelationId(future);
            try {
                write(channel, new WriteUtil.Writer() {
                    public void write(final DataOutput output) throws IOException {
                        output.writeByte(getCommandId());
                        output.writeInt(correlationId);

                        final Marshaller marshaller = prepareForMarshalling(output);
                        marshaller.writeByte(NAME);
                        marshaller.writeObject(name);
                        marshaller.finish();
                    }
                });

                final IoFuture.Status result = future.await(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                switch (result) {
                    case FAILED:
                        if (future.getHeldException() != null) {
                            throw future.getHeldException();
                        }
                        throw future.getException();
                    case DONE:
                        return null;
                    default:
                        throw new NamingException("Unable to invoke unbind, status=" + result.toString());
                }

            } catch (NamingException e) {
                throw e;
            } catch (Exception e) {
                throw namingException("Failed to unbind", e);
            } finally {
                releaseCorrelationId(correlationId);
            }
        }

        public void handleServerMessage(final Channel channel, final DataInput input, final int correlationId, final RemoteNamingService remoteNamingService) throws IOException {
            final Unmarshaller unmarshaller = prepareForUnMarshalling(input);
            Name name;
            try {
                byte paramType = unmarshaller.readByte();
                if (paramType != NAME) {
                    remoteNamingService.getLogger().unexpectedParameterType(NAME, paramType);
                }
                name = unmarshaller.readObject(Name.class);
            } catch (ClassNotFoundException cnfe) {
                throw new IOException(cnfe);
            } finally {
                unmarshaller.finish();
            }

            try {
                remoteNamingService.getLocalContext().unbind(name);
                writeResponse(channel, getCommandId(), correlationId);
            } catch (NamingException e) {
                writeExceptionResponse(channel, e, getCommandId(), correlationId);
            }
        }

        public void handleClientMessage(DataInput input, int correlationId, RemoteNamingStore namingStore) throws IOException {
            readResult(correlationId, input, new ValueReader<Object>() {
                public void read(final DataInput input, ProtocolIoFuture<Object> future) throws IOException {
                    future.setResult(null);
                }
            });
        }
    };

    static ProtocolCommand<Void> RENAME = new BaseProtocolCommand<Void>((byte) 0x07) {
        public Void execute(final Channel channel, final Object... args) throws IOException, NamingException {
            if (args.length != 2 || !(args[0] instanceof Name) || !(args[0] instanceof Name)) {
                throw new IllegalArgumentException("Rename requires two name arguments");
            }
            final Name name = Name.class.cast(args[0]);
            final Name newName = Name.class.cast(args[1]);

            final ProtocolIoFuture<Void> future = new ProtocolIoFuture<Void>();
            final int correlationId = reserveNextCorrelationId(future);
            try {
                write(channel, new WriteUtil.Writer() {
                    public void write(final DataOutput output) throws IOException {
                        output.writeByte(getCommandId());
                        output.writeInt(correlationId);

                        final Marshaller marshaller = prepareForMarshalling(output);
                        marshaller.writeByte(NAME);
                        marshaller.writeObject(name);
                        marshaller.writeByte(NAME);
                        marshaller.writeObject(newName);
                        marshaller.finish();
                    }
                });

                final IoFuture.Status result = future.await(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                switch (result) {
                    case FAILED:
                        if (future.getHeldException() != null) {
                            throw future.getHeldException();
                        }
                        throw future.getException();
                    case DONE:
                        return null;
                    default:
                        throw new NamingException("Unable to invoke rename, status=" + result.toString());
                }

            } catch (NamingException e) {
                throw e;
            } catch (Exception e) {
                throw namingException("Failed to rename", e);
            } finally {
                releaseCorrelationId(correlationId);
            }
        }

        public void handleServerMessage(final Channel channel, final DataInput input, final int correlationId, final RemoteNamingService remoteNamingService) throws IOException {
            final Unmarshaller unmarshaller = prepareForUnMarshalling(input);
            final Name name;
            final Name newName;
            try {
                byte paramType = unmarshaller.readByte();
                if (paramType != NAME) {
                    remoteNamingService.getLogger().unexpectedParameterType(NAME, paramType);
                }
                name = unmarshaller.readObject(Name.class);

                paramType = unmarshaller.readByte();
                if (paramType != NAME) {
                    remoteNamingService.getLogger().unexpectedParameterType(OBJECT, paramType);
                }
                newName = unmarshaller.readObject(Name.class);
            } catch (ClassNotFoundException cnfe) {
                throw new IOException(cnfe);
            } finally {
                unmarshaller.finish();
            }

            try {
                remoteNamingService.getLocalContext().rename(name, newName);
                writeResponse(channel, getCommandId(), correlationId);
            } catch (NamingException e) {
                writeExceptionResponse(channel, e, getCommandId(), correlationId);
            }
        }

        public void handleClientMessage(DataInput input, int correlationId, RemoteNamingStore namingStore) throws IOException {
            readResult(correlationId, input, new ValueReader<Object>() {
                public void read(final DataInput input, ProtocolIoFuture<Object> future) throws IOException {
                    future.setResult(null);
                }
            });
        }
    };

    static ProtocolCommand<Context> CREATE_SUBCONTEXT = new BaseProtocolCommand<Context>((byte) 0x08) {

        public Context execute(final Channel channel, final Object... args) throws IOException, NamingException {
            if (args.length != 1 || !(args[0] instanceof Name)) {
                throw new IllegalArgumentException("Create subcontext requires a single name argument");
            }
            final Name name = Name.class.cast(args[0]);
            final NamedIoFuture<Context> future = new NamedIoFuture<Context>(name);
            final int correlationId = reserveNextCorrelationId(future);
            try {
                write(channel, new WriteUtil.Writer() {
                    public void write(final DataOutput output) throws IOException {
                        output.writeByte(getCommandId());
                        output.writeInt(correlationId);

                        final Marshaller marshaller = prepareForMarshalling(output);
                        marshaller.writeByte(NAME);
                        marshaller.writeObject(name);
                        marshaller.finish();
                    }
                });

                final IoFuture.Status result = future.await(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                switch (result) {
                    case FAILED:
                        if (future.getHeldException() != null) {
                            throw future.getHeldException();
                        }
                        throw future.getException();
                    case DONE:
                        return future.get();
                    default:
                        throw new NamingException("Unable to invoke createSubcontext, status=" + result.toString());
                }

            } catch (NamingException e) {
                throw e;
            } catch (Exception e) {
                throw namingException("Failed to create subcontext", e);
            } finally {
                releaseCorrelationId(correlationId);
            }
        }

        public void handleServerMessage(final Channel channel, final DataInput input, final int correlationId, final RemoteNamingService remoteNamingService) throws IOException {

            Name name;
            try {
                final Unmarshaller unmarshaller = prepareForUnMarshalling(input);
                byte paramType = unmarshaller.readByte();
                if (paramType != NAME) {
                    remoteNamingService.getLogger().unexpectedParameterType(NAME, paramType);
                }
                name = unmarshaller.readObject(Name.class);
                unmarshaller.finish();
            } catch (ClassNotFoundException cnfe) {
                throw new IOException(cnfe);
            }

            try {
                remoteNamingService.getLocalContext().createSubcontext(name);
                write(channel, new WriteUtil.Writer() {
                    public void write(DataOutput output) throws IOException {
                        output.writeByte(getCommandId());
                        output.writeInt(correlationId);
                        output.writeByte(SUCCESS);
                        output.writeByte(CONTEXT);
                    }
                });
            } catch (NamingException e) {
                writeExceptionResponse(channel, e, getCommandId(), correlationId);
            }
        }

        public void handleClientMessage(final DataInput input, final int correlationId, final RemoteNamingStore namingStore) throws IOException {
            readResult(correlationId, input, new ValueReader<Context>() {
                public void read(final DataInput input, final ProtocolIoFuture<Context> future) throws IOException {
                    final byte parameterType = input.readByte();
                    if (parameterType != CONTEXT) {
                        throw new IOException("Unexpected paramType");
                    }
                    future.setResult(new RemoteContext(NamedIoFuture.class.cast(future).name, namingStore, new Hashtable<String, Object>()));
                }
            });
        }
    };

    static ProtocolCommand<Object> DESTROY_SUBCONTEXT = new BaseProtocolCommand<Object>((byte) 0x09) {

        public Context execute(final Channel channel, final Object... args) throws IOException, NamingException {
            if (args.length != 1 || !(args[0] instanceof Name)) {
                throw new IllegalArgumentException("Destroy subcontext requires a single name argument");
            }
            final Name name = Name.class.cast(args[0]);
            final NamedIoFuture<Context> future = new NamedIoFuture<Context>(name);
            final int correlationId = reserveNextCorrelationId(future);
            try {
                write(channel, new WriteUtil.Writer() {
                    public void write(final DataOutput output) throws IOException {
                        output.writeByte(getCommandId());
                        output.writeInt(correlationId);

                        final Marshaller marshaller = prepareForMarshalling(output);
                        marshaller.writeByte(NAME);
                        marshaller.writeObject(name);
                        marshaller.finish();
                    }
                });

                final IoFuture.Status result = future.await(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                switch (result) {
                    case FAILED:
                        if (future.getHeldException() != null) {
                            throw future.getHeldException();
                        }
                        throw future.getException();
                    case DONE:
                        return future.get();
                    default:
                        throw new NamingException("Unable to invoke destroySubcontext, status=" + result.toString());
                }

            } catch (NamingException e) {
                throw e;
            } catch (Exception e) {
                throw namingException("Failed to destroy subcontext", e);
            } finally {
                releaseCorrelationId(correlationId);
            }
        }

        public void handleServerMessage(Channel channel, final DataInput input, final int correlationId, final RemoteNamingService remoteNamingService) throws IOException {

            Name name;
            try {
                final Unmarshaller unmarshaller = prepareForUnMarshalling(input);
                byte paramType = unmarshaller.readByte();
                if (paramType != NAME) {
                    remoteNamingService.getLogger().unexpectedParameterType(NAME, paramType);
                }
                name = unmarshaller.readObject(Name.class);
                unmarshaller.finish();
            } catch (ClassNotFoundException cnfe) {
                throw new IOException(cnfe);
            }

            try {
                remoteNamingService.getLocalContext().destroySubcontext(name);
                writeResponse(channel, getCommandId(), correlationId);
            } catch (NamingException e) {
                writeExceptionResponse(channel, e, getCommandId(), correlationId);
            }
        }

        public void handleClientMessage(final DataInput input, final int correlationId, final RemoteNamingStore namingStore) throws IOException {
            readResult(correlationId, input, new ValueReader<Object>() {
                public void read(final DataInput input, ProtocolIoFuture<Object> future) throws IOException {
                    future.setResult(null);
                }
            });
        }
    };

    static ProtocolCommand<Object> LOOKUP_LINK = new BaseProtocolCommand<Object>((byte) 0x10) {
        public Object execute(final Channel channel, final Object... args) throws IOException, NamingException {
            if (args.length != 1 || !(args[0] instanceof Name)) {
                throw new IllegalArgumentException("Lookup link requires a single name argument");
            }
            final Name name = Name.class.cast(args[0]);
            final NamedIoFuture<Object> future = new NamedIoFuture<Object>(name);
            final int correlationId = reserveNextCorrelationId(future);
            try {
                write(channel, new WriteUtil.Writer() {
                    public void write(final DataOutput output) throws IOException {
                        output.writeByte(getCommandId());
                        output.writeInt(correlationId);

                        final Marshaller marshaller = prepareForMarshalling(output);
                        marshaller.writeByte(NAME);
                        marshaller.writeObject(name);
                        marshaller.finish();
                    }
                });

                final IoFuture.Status result = future.await(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                switch (result) {
                    case FAILED:
                        if (future.getHeldException() != null) {
                            throw future.getHeldException();
                        }
                        throw future.getException();
                    case DONE:
                        return future.get();
                    default:
                        throw new NamingException("Unable to invoke lookupLink, status=" + result.toString());
                }

            } catch (NamingException e) {
                throw e;
            } catch (Exception e) {
                throw namingException("Failed to lookup link", e);
            } finally {
                releaseCorrelationId(correlationId);
            }
        }

        public void handleServerMessage(Channel channel, final DataInput input, final int correlationId, final RemoteNamingService remoteNamingService) throws IOException {

            final Unmarshaller unmarshaller = prepareForUnMarshalling(input);
            Name name;
            try {
                byte paramType = unmarshaller.readByte();
                if (paramType != NAME) {
                    remoteNamingService.getLogger().unexpectedParameterType(NAME, paramType);
                }
                name = unmarshaller.readObject(Name.class);
            } catch (ClassNotFoundException cnfe) {
                throw new IOException(cnfe);
            } finally {
                unmarshaller.close();
            }

            try {
                final Object result = remoteNamingService.getLocalContext().lookupLink(name);
                write(channel, new WriteUtil.Writer() {
                    public void write(DataOutput output) throws IOException {
                        output.writeByte(getCommandId());
                        output.writeInt(correlationId);
                        output.writeByte(SUCCESS);
                        if (result instanceof Context) {
                            output.writeByte(CONTEXT);
                        } else {
                            output.writeByte(BINDING);
                            final Marshaller marshaller = prepareForMarshalling(output);
                            marshaller.writeObject(result);
                            marshaller.finish();
                        }
                    }
                });
            } catch (NamingException e) {
                writeExceptionResponse(channel, e, getCommandId(), correlationId);
            }
        }

        public void handleClientMessage(final DataInput input, final int correlationId, final RemoteNamingStore namingStore) throws IOException {
            readResult(correlationId, input, new ValueReader<Object>() {
                public void read(final DataInput input, ProtocolIoFuture<Object> future) throws IOException {
                    byte parameterType = input.readByte();
                    switch (parameterType) {
                        case BINDING: {
                            try {
                                final Unmarshaller unmarshaller = prepareForUnMarshalling(input);
                                future.setResult(unmarshaller.readObject());
                                unmarshaller.finish();
                            } catch (ClassNotFoundException e) {
                                throw new IOException(e);
                            } catch (ClassCastException e) {
                                throw new IOException(e);
                            }
                            break;
                        }
                        case CONTEXT: {
                            future.setResult(new RemoteContext(NamedIoFuture.class.cast(future).name, namingStore, new Hashtable<String, Object>()));
                            break;
                        }
                        default: {
                            throw new IOException("Unexpected response parameter received.");
                        }

                    }
                }
            });
        }
    };

    private static class NamedIoFuture<T> extends ProtocolIoFuture<T> {
        private final Name name;

        private NamedIoFuture(Name name) {
            this.name = name;
        }
    }

    private static final Map<Byte, ProtocolCommand> commands = new HashMap<Byte, ProtocolCommand>();

    static void register(final ProtocolCommand<?> command) {
        commands.put(command.getCommandId(), command);
    }

    static {
        register(LOOKUP);
        register(BIND);
        register(REBIND);
        register(LIST);
        register(LIST_BINDINGS);
        register(UNBIND);
        register(RENAME);
        register(CREATE_SUBCONTEXT);
        register(DESTROY_SUBCONTEXT);
        register(LOOKUP_LINK);
    }

    public static ProtocolCommand forId(final byte id) {
        return commands.get(id);
    }
}
