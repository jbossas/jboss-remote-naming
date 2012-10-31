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
package org.jboss.naming.remote.common.ejb;

import org.jboss.ejb.client.ClusterAffinity;
import org.jboss.ejb.client.SessionID;
import org.jboss.ejb.client.remoting.PackedInteger;
import org.jboss.logging.Logger;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.SimpleDataInput;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.OpenListener;
import org.jboss.remoting3.Registration;
import org.xnio.IoUtils;
import org.xnio.OptionMap;
import org.xnio.channels.AcceptingChannel;
import org.xnio.channels.ConnectedStreamChannel;

import javax.ejb.NoSuchEJBException;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Future;

/**
 * @author Jaikiran Pai
 */
public class DummyEJBServer {

    private static final Logger logger = Logger.getLogger(DummyEJBServer.class);

    private static final String[] supportedMarshallerTypes = new String[]{"river", "java-serial"};
    private static final String CLUSTER_NAME = "dummy-cluster";

    private AcceptingChannel<? extends ConnectedStreamChannel> server;
    private Map<EJBModuleIdentifier, Map<String, Object>> registeredEJBs = new ConcurrentHashMap<EJBModuleIdentifier, Map<String, Object>>();

    private final Collection<Channel> openChannels = new CopyOnWriteArraySet<Channel>();

    private final Endpoint endpoint;
    private volatile Registration ejbChannelRegistration;

    public DummyEJBServer(final Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    public synchronized void start() throws IOException {
        if (this.ejbChannelRegistration != null) {
            throw new IllegalStateException(this.getClass().getSimpleName() + " is already started");
        }
        this.ejbChannelRegistration = this.registerEJBServer();
    }

    public synchronized void stop() throws IOException {
        if (this.ejbChannelRegistration == null) {
            throw new IllegalStateException(this.getClass().getSimpleName() + " is not started");
        }
        this.ejbChannelRegistration.close();
    }

    private Registration registerEJBServer() throws IOException {
        logger.info("Registering EJB server to endpoint " + endpoint);
        return endpoint.registerService("jboss.ejb", new OpenListener() {
            @Override
            public void channelOpened(Channel channel) {
                logger.info("Channel opened " + channel);
                channel.addCloseHandler(new CloseHandler<Channel>() {
                    @Override
                    public void handleClose(Channel closed, IOException exception) {
                        logger.info("Bye " + closed);
                    }
                });
                try {
                    this.sendVersionMessage(channel);
                } catch (IOException e) {
                    logger.error("Could not send version message to channel " + channel + " Closing the channel");
                    IoUtils.safeClose(channel);
                }
                Channel.Receiver handler = new VersionReceiver();
                channel.receiveMessage(handler);
            }

            @Override
            public void registrationTerminated() {
                logger.info("Registration terminated for open listener");
            }

            private void sendVersionMessage(final Channel channel) throws IOException {
                final DataOutputStream outputStream = new DataOutputStream(channel.writeMessage());
                // write the version
                outputStream.write(0x01);
                // write the marshaller type count
                PackedInteger.writePackedInteger(outputStream, supportedMarshallerTypes.length);
                // write the marshaller types
                for (int i = 0; i < supportedMarshallerTypes.length; i++) {
                    outputStream.writeUTF(supportedMarshallerTypes[i]);
                }
                outputStream.flush();
                outputStream.close();
            }

        }, OptionMap.EMPTY);
    }

    public String getClusterName() {
        return this.CLUSTER_NAME;
    }


    class Version1Receiver implements Channel.Receiver {

        private final DummyProtocolHandler dummyProtocolHandler;

        Version1Receiver(final String marshallingType) {
            this.dummyProtocolHandler = new DummyProtocolHandler(marshallingType);
        }

        @Override
        public void handleError(Channel channel, IOException error) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void handleEnd(Channel channel) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void handleMessage(Channel channel, MessageInputStream messageInputStream) {
            final DataInputStream inputStream = new DataInputStream(messageInputStream);
            try {
                final byte header = inputStream.readByte();
                logger.info("Received message with header 0x" + Integer.toHexString(header));
                switch (header) {
                    case 0x03:
                        final MethodInvocationRequest methodInvocationRequest = this.dummyProtocolHandler.readMethodInvocationRequest(inputStream, this.getClass().getClassLoader());
                        Object methodInvocationResult = null;
                        try {
                            methodInvocationResult = DummyEJBServer.this.handleMethodInvocationRequest(channel, methodInvocationRequest, dummyProtocolHandler);
                        } catch (NoSuchEJBException nsee) {
                            final DataOutputStream outputStream = new DataOutputStream(channel.writeMessage());
                            try {
                                this.dummyProtocolHandler.writeNoSuchEJBFailureMessage(outputStream, methodInvocationRequest.getInvocationId(), methodInvocationRequest.getAppName(),
                                        methodInvocationRequest.getModuleName(), methodInvocationRequest.getDistinctName(), methodInvocationRequest.getBeanName(),
                                        methodInvocationRequest.getViewClassName());
                            } finally {
                                outputStream.close();
                            }
                            return;
                        } catch (Exception e) {
                            final DataOutputStream outputStream = new DataOutputStream(channel.writeMessage());
                            try {
                                this.dummyProtocolHandler.writeException(outputStream, methodInvocationRequest.getInvocationId(), e, methodInvocationRequest.getAttachments());
                            } finally {
                                outputStream.close();
                            }
                            return;
                        }
                        logger.info("Method invocation result on server " + methodInvocationResult);
                        // write the method invocation result
                        final DataOutputStream outputStream = new DataOutputStream(channel.writeMessage());
                        try {
                            this.dummyProtocolHandler.writeMethodInvocationResponse(outputStream, methodInvocationRequest.getInvocationId(), methodInvocationResult, methodInvocationRequest.getAttachments());
                        } finally {
                            outputStream.close();
                        }

                        break;
                    case 0x01:
                        // session open request
                        try {
                            this.handleSessionOpenRequest(channel, messageInputStream);
                        } catch (Exception e) {
                            // TODO: Let the client know of this exception
                            throw new RuntimeException(e);
                        }

                        break;
                    default:
                        logger.warn("Not supported message header 0x" + Integer.toHexString(header) + " received by " + this);
                        return;
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                // receive next message
                channel.receiveMessage(this);
                IoUtils.safeClose(inputStream);
            }

        }

        private void handleSessionOpenRequest(Channel channel, MessageInputStream messageInputStream) throws IOException {
            if (messageInputStream == null) {
                throw new IllegalArgumentException("Cannot read from null message inputstream");
            }
            final DataInputStream dataInputStream = new DataInputStream(messageInputStream);

            // read invocation id
            final short invocationId = dataInputStream.readShort();
            final String appName = dataInputStream.readUTF();
            final String moduleName = dataInputStream.readUTF();
            final String distinctName = dataInputStream.readUTF();
            final String beanName = dataInputStream.readUTF();

            final EJBModuleIdentifier ejbModuleIdentifier = new EJBModuleIdentifier(appName, moduleName, distinctName);
            final Map<String, Object> ejbs = DummyEJBServer.this.registeredEJBs.get(ejbModuleIdentifier);
            if (ejbs == null || ejbs.get(beanName) == null) {
                final DataOutputStream outputStream = new DataOutputStream(channel.writeMessage());
                try {
                    this.dummyProtocolHandler.writeNoSuchEJBFailureMessage(outputStream, invocationId, appName, moduleName, distinctName, beanName, null);
                } finally {
                    outputStream.close();
                }
                return;
            }
            final UUID uuid = UUID.randomUUID();
            ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
            bb.putLong(uuid.getMostSignificantBits());
            bb.putLong(uuid.getLeastSignificantBits());
            final SessionID sessionID = SessionID.createSessionID(bb.array());
            final DataOutputStream outputStream = new DataOutputStream(channel.writeMessage());
            try {
                final ClusterAffinity hardAffinity = new ClusterAffinity(DummyEJBServer.this.CLUSTER_NAME);
                this.dummyProtocolHandler.writeSessionId(outputStream, invocationId, sessionID, hardAffinity);
            } finally {
                outputStream.close();
            }
        }


    }

    public void register(final String appName, final String moduleName, final String distinctName, final String beanName, final Object instance) {

        final EJBModuleIdentifier moduleID = new EJBModuleIdentifier(appName, moduleName, distinctName);
        Map<String, Object> ejbs = this.registeredEJBs.get(moduleID);
        if (ejbs == null) {
            ejbs = new HashMap<String, Object>();
            this.registeredEJBs.put(moduleID, ejbs);
        }
        ejbs.put(beanName, instance);
        try {
            this.sendNewModuleReportToClients(new EJBModuleIdentifier[]{moduleID}, true);
        } catch (IOException e) {
            logger.warn("Could not send EJB module availability message to clients, for module " + moduleID, e);
        }
    }

    public void unregister(final String appName, final String moduleName, final String distinctName, final String beanName) {
        this.unregister(appName, moduleName, distinctName, beanName, true);
    }

    public void unregister(final String appName, final String moduleName, final String distinctName, final String beanName, final boolean notifyClients) {

        final EJBModuleIdentifier moduleID = new EJBModuleIdentifier(appName, moduleName, distinctName);
        Map<String, Object> ejbs = this.registeredEJBs.get(moduleID);
        if (ejbs != null) {
            ejbs.remove(beanName);
        }
        if (notifyClients) {
            try {
                this.sendNewModuleReportToClients(new EJBModuleIdentifier[]{moduleID}, false);
            } catch (IOException e) {
                logger.warn("Could not send EJB module un-availability message to clients, for module " + moduleID, e);
            }
        }
    }


    private void sendNewModuleReportToClients(final EJBModuleIdentifier[] modules, final boolean availabilityReport) throws IOException {
        if (modules == null) {
            return;
        }
        if (this.openChannels.isEmpty()) {
            logger.debug("No open channels to send EJB module availability");
        }
        for (final Channel channel : this.openChannels) {
            final DataOutputStream dataOutputStream = new DataOutputStream(channel.writeMessage());
            try {
                if (availabilityReport) {
                    this.writeModuleAvailability(dataOutputStream, modules);
                } else {
                    this.writeModuleUnAvailability(dataOutputStream, modules);
                }
            } catch (IOException e) {
                logger.warn("Could not send module availability message to client", e);
            } finally {
                dataOutputStream.close();
            }

        }
    }

    private void writeModuleAvailability(final DataOutput output, final EJBModuleIdentifier[] ejbModuleIdentifiers) throws IOException {
        if (output == null) {
            throw new IllegalArgumentException("Cannot write to null output");
        }
        if (ejbModuleIdentifiers == null) {
            throw new IllegalArgumentException("EJB module identifiers cannot be null");
        }
        // write the header
        output.write(0x08);
        this.writeModuleReport(output, ejbModuleIdentifiers);
    }

    private void writeModuleUnAvailability(final DataOutput output, final EJBModuleIdentifier[] ejbModuleIdentifiers) throws IOException {
        if (output == null) {
            throw new IllegalArgumentException("Cannot write to null output");
        }
        if (ejbModuleIdentifiers == null) {
            throw new IllegalArgumentException("EJB module identifiers cannot be null");
        }
        // write the header
        output.write(0x09);
        this.writeModuleReport(output, ejbModuleIdentifiers);
    }

    private void writeModuleReport(final DataOutput output, final EJBModuleIdentifier[] modules) throws IOException {
        // write the count
        PackedInteger.writePackedInteger(output, modules.length);
        // write the module identifiers
        for (int i = 0; i < modules.length; i++) {
            // write the app name
            final String appName = modules[i].getAppName();
            if (appName == null) {
                // write out a empty string
                output.writeUTF("");
            } else {
                output.writeUTF(appName);
            }
            // write the module name
            output.writeUTF(modules[i].getModuleName());
            // write the distinct name
            final String distinctName = modules[i].getDistinctName();
            if (distinctName == null) {
                // write out an empty string
                output.writeUTF("");
            } else {
                output.writeUTF(distinctName);
            }
        }
    }

    private Object handleMethodInvocationRequest(final Channel channel, final MethodInvocationRequest methodInvocationRequest, final DummyProtocolHandler dummyProtocolHandler) throws InvocationTargetException, IllegalAccessException, IOException {
        final EJBModuleIdentifier ejbModuleIdentifier = new EJBModuleIdentifier(methodInvocationRequest.getAppName(), methodInvocationRequest.getModuleName(), methodInvocationRequest.getDistinctName());
        final Map<String, Object> ejbs = this.registeredEJBs.get(ejbModuleIdentifier);
        final Object beanInstance = ejbs.get(methodInvocationRequest.getBeanName());
        if (beanInstance == null) {
            throw new NoSuchEJBException(methodInvocationRequest.getBeanName() + " EJB not available");
        }
        Method method = null;
        try {
            method = this.getRequiredMethod(beanInstance.getClass(), methodInvocationRequest.getMethodName(), methodInvocationRequest.getParamTypes());
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        // check if this is an async method
        if (this.isAsyncMethod(method)) {
            final DataOutputStream output = new DataOutputStream(channel.writeMessage());
            try {
                // send a notification to the client that this is an async method
                dummyProtocolHandler.writeAsyncMethodNotification(output, methodInvocationRequest.getInvocationId());
            } finally {
                output.close();
            }
        }
        // invoke on the method
        return method.invoke(beanInstance, methodInvocationRequest.getParams());
    }

    private Method getRequiredMethod(final Class<?> klass, final String methodName, final String[] paramTypes) throws NoSuchMethodException {
        final Class<?>[] types = new Class<?>[paramTypes.length];
        for (int i = 0; i < paramTypes.length; i++) {
            try {
                types[i] = Class.forName(paramTypes[i], false, klass.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        return klass.getMethod(methodName, types);
    }

    private boolean isAsyncMethod(final Method method) {
        // just check for return type and assume it to be async if it returns Future
        return method.getReturnType().equals(Future.class);
    }

    class VersionReceiver implements Channel.Receiver {
        @Override
        public void handleError(Channel channel, IOException error) {
            try {
                channel.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            throw new RuntimeException("NYI: .handleError");
        }

        @Override
        public void handleEnd(Channel channel) {
            try {
                channel.close();
            } catch (IOException e) {
                // ignore
            }
        }


        @Override
        public void handleMessage(Channel channel, MessageInputStream message) {
            final SimpleDataInput input = new SimpleDataInput(Marshalling.createByteInput(message));
            try {
                final byte version = input.readByte();
                final String clientMarshallingType = input.readUTF();
                input.close();
                switch (version) {
                    case 0x01:
                        final Version1Receiver receiver = new Version1Receiver(clientMarshallingType);
                        DummyEJBServer.this.openChannels.add(channel);
                        channel.receiveMessage(receiver);
                        // send module availability report to clients
                        final Collection<EJBModuleIdentifier> availableModules = DummyEJBServer.this.registeredEJBs.keySet();
                        DummyEJBServer.this.sendNewModuleReportToClients(availableModules.toArray(new EJBModuleIdentifier[availableModules.size()]), true);
                        break;
                    default:
                        logger.info("Received unsupported version 0x" + Integer.toHexString(version) + " from client, on channel " + channel);
                        channel.close();
                        break;
                }
            } catch (IOException e) {
                logger.error("Exception on channel " + channel, e);
                try {
                    logger.info("Shutting down channel " + channel);
                    channel.writeShutdown();
                } catch (IOException e1) {
                    // ignore
                    if (logger.isTraceEnabled()) {
                        logger.trace("Ignoring exception that occurred during channel shutdown", e1);
                    }
                }
            }
        }
    }


    private class EJBModuleIdentifier {
        private final String appName;

        private final String moduleName;

        private final String distinctName;

        EJBModuleIdentifier(final String appname, final String moduleName, final String distinctName) {
            this.appName = appname;
            this.moduleName = moduleName;
            this.distinctName = distinctName;
        }

        String getAppName() {
            return this.appName;
        }

        String getModuleName() {
            return this.moduleName;
        }

        String getDistinctName() {
            return this.distinctName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            EJBModuleIdentifier that = (EJBModuleIdentifier) o;

            if (appName != null ? !appName.equals(that.appName) : that.appName != null) return false;
            if (distinctName != null ? !distinctName.equals(that.distinctName) : that.distinctName != null)
                return false;
            if (!moduleName.equals(that.moduleName)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = appName != null ? appName.hashCode() : 0;
            result = 31 * result + moduleName.hashCode();
            result = 31 * result + (distinctName != null ? distinctName.hashCode() : 0);
            return result;
        }
    }
}
