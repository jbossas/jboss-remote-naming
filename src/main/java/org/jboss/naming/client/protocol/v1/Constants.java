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
package org.jboss.naming.client.protocol.v1;

/**
 * The version 1 constants.
 *
 * @author John Bailey
 */
class Constants {
    /*
     * Outcomes
     */
    static final byte SUCCESS = 0x00;
    static final byte FAILURE = 0x01;

    /*
    * Parameter Types
    */
    static final byte NAME = 0x00;
    static final byte OBJECT = 0x01;
    static final byte EXCEPTION = 0x02;
    static final byte VOID = 0x03;
    static final byte BINDING = 0x04;
    static final byte CONTEXT = 0x05;
    static final byte LIST = 0x06;

    /*
     * General
     */
    static final String MARSHALLING_STRATEGY = "river";


}
