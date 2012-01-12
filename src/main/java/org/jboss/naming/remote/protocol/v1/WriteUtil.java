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

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import org.jboss.marshalling.ByteOutput;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.MarshallerFactory;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.naming.remote.protocol.CancellableDataOutputStream;
import static org.jboss.naming.remote.protocol.v1.Constants.EXCEPTION;
import static org.jboss.naming.remote.protocol.v1.Constants.FAILURE;
import static org.jboss.naming.remote.protocol.v1.Constants.MARSHALLING_STRATEGY;
import static org.jboss.naming.remote.protocol.v1.Constants.OBJECT;
import static org.jboss.naming.remote.protocol.v1.Constants.SUCCESS;
import org.jboss.remoting3.Channel;
import org.xnio.IoUtils;

/**
 * @author John Bailey
 */
class WriteUtil {
    static final MarshallerFactory marshallerFactory;

    static {
        marshallerFactory = Marshalling.getProvidedMarshallerFactory(MARSHALLING_STRATEGY);
        if (marshallerFactory == null) {
            throw new RuntimeException("Could not find a marshaller factory for " + MARSHALLING_STRATEGY + " marshalling strategy");
        }
    }

    static interface Writer {
        void write(DataOutput output) throws IOException;
    }

    static void write(final Channel channel, Writer writer) throws IOException {
        CancellableDataOutputStream output = new CancellableDataOutputStream(channel.writeMessage());
        try {
            writer.write(output);
        } catch (IOException e) {
            output.cancel();
            throw e;
        } finally {
            IoUtils.safeClose(output);
        }
    }

    static void writeResponse(final Channel channel, final byte command, final int correlationId) throws IOException {
        write(channel, new Writer() {
            public void write(DataOutput output) throws IOException {
                output.writeByte(command);
                output.writeInt(correlationId);
                output.writeByte(SUCCESS);
            }
        });
    }

    static void writeExceptionResponse(final Channel channel, final Exception e, final byte command, final int correlationId) throws IOException {
        write(channel, new Writer() {
            public void write(DataOutput output) throws IOException {
                output.writeByte(command);
                output.writeInt(correlationId);
                output.writeByte(FAILURE);
                output.writeByte(EXCEPTION);

                Marshaller marshaller = prepareForMarshalling(output);
                marshaller.writeObject(e);
                marshaller.finish();
            }
        });

    }

    static void writeResponse(final Channel channel, final Object response, final byte command, final int correlationId) throws IOException {
        write(channel, new Writer() {
            public void write(DataOutput output) throws IOException {
                output.writeByte(command);
                output.writeInt(correlationId);
                output.writeByte(SUCCESS);
                output.writeByte(OBJECT);

                Marshaller marshaller = prepareForMarshalling(output);
                marshaller.writeObject(response);
                marshaller.finish();
            }
        });

    }

    static Marshaller prepareForMarshalling(final DataOutput dataOutput) throws IOException {
        final Marshaller marshaller = getMarshaller(marshallerFactory);
        final OutputStream outputStream = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                final int byteToWrite = b & 0xff;
                dataOutput.write(byteToWrite);
            }
        };
        final ByteOutput byteOutput = Marshalling.createByteOutput(outputStream);
        // start the marshaller
        marshaller.start(byteOutput);

        return marshaller;
    }

    static Marshaller getMarshaller(final MarshallerFactory marshallerFactory) throws IOException {
        final MarshallingConfiguration marshallingConfiguration = new MarshallingConfiguration();
        marshallingConfiguration.setVersion(2);
        return marshallerFactory.createMarshaller(marshallingConfiguration);
    }
}
