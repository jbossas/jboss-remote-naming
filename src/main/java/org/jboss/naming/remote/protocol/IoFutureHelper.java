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

package org.jboss.naming.remote.protocol;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.sasl.SaslException;

import org.xnio.IoFuture;

public class IoFutureHelper {
    public static <V> Future<V> future(final IoFuture<V> ioFuture) {
        return new Future<V>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return ioFuture.cancel().getStatus() == IoFuture.Status.CANCELLED;
            }

            @Override
            public boolean isCancelled() {
                return ioFuture.getStatus() == IoFuture.Status.CANCELLED;
            }

            @Override
            public boolean isDone() {
                return ioFuture.getStatus() == IoFuture.Status.DONE;
            }

            @Override
            public V get() throws InterruptedException, ExecutionException {
                final IoFuture.Status status = ioFuture.awaitInterruptibly();
                return result(status);
            }

            @Override
            public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                final IoFuture.Status status = ioFuture.awaitInterruptibly(timeout, unit);
                if (status == IoFuture.Status.WAITING)
                    throw new TimeoutException("Operation timed out");
                return result(status);
            }

            private V result(final IoFuture.Status status) throws ExecutionException {
                switch (status) {
                    case CANCELLED:
                        throw new CancellationException();
                    case FAILED:
                        throw new ExecutionException(ioFuture.getException());
                }
                try {
                    return ioFuture.get();
                } catch (IOException e) {
                    throw new ExecutionException(e);
                }
            }
        };
    }

    public static <V> V get(final IoFuture<V> ioFuture, final long timeout, final TimeUnit unit) throws IOException {
        final IoFuture.Status status = ioFuture.await(timeout, unit);
        switch (status) {
            case DONE:
                return ioFuture.get();
            case FAILED:
            	// SaslException is an IOException, which indicates a security or authentication issue, maybe should throw AuthenticationException
            	if(ioFuture.getException() instanceof SaslException)
            		throw ioFuture.getException();
            	// if it is not a SaslException wrap it in a RuntimeException, should we just wrap it in an IOException ?
                throw new RuntimeException(ioFuture.getException());
            case WAITING:
            	throw new ConnectException("Operation failed with status " + status + " after " + timeout + " " + unit);
        }
        // Should we wrap it in an IOException, since we are already throwing it?
        throw new RuntimeException("Operation failed with status " + status);
    }
}
