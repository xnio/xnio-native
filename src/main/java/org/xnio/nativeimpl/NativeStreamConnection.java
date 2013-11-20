/*
 * JBoss, Home of Professional Open Source
 *
 * Copyright 2013 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.xnio.nativeimpl;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Set;

import org.xnio.Option;
import org.xnio.Options;
import org.xnio.StreamConnection;
import org.xnio.conduits.StreamSinkConduit;
import org.xnio.conduits.StreamSourceConduit;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
abstract class NativeStreamConnection extends StreamConnection {

    final int fd;
    final NativeStreamConduit conduit;

    protected NativeStreamConnection(final NativeWorkerThread thread, final int fd) {
        super(thread);
        this.fd = fd;
        this.conduit = constructConduit(thread, fd);
        setSourceConduit(conduit);
        setSinkConduit(conduit);
    }

    protected NativeStreamConduit constructConduit(final NativeWorkerThread thread, final int fd) {
        return new NativeStreamConduit(thread, fd, this);
    }

    public SocketAddress getPeerAddress() {
        return Native.getSocketAddress(Native.getPeerName(fd));
    }

    public SocketAddress getLocalAddress() {
        return Native.getSocketAddress(Native.getSockName(fd));
    }

    NativeStreamConduit getConduit() {
        return conduit;
    }

    private static final Set<Option<?>> OPTIONS = Option.setBuilder()
            .add(Options.READ_TIMEOUT)
            .add(Options.SEND_BUFFER)
            .add(Options.WRITE_TIMEOUT)
            .create();

    public boolean supportsOption(final Option<?> option) {
        return OPTIONS.contains(option) || super.supportsOption(option);
    }

    public <T> T getOption(final Option<T> option) throws IOException {
        if (option == Options.READ_TIMEOUT) {
            return option.cast(Integer.valueOf(conduit.getReadTimeout()));
        } else if (option == Options.WRITE_TIMEOUT) {
            return option.cast(Integer.valueOf(conduit.getWriteTimeout()));
        } else if (option == Options.SEND_BUFFER) {
            return option.cast(Integer.valueOf(Native.testAndThrow(Native.getOptSendBuffer(fd))));
        } else {
            return super.getOption(option);
        }
    }

    public <T> T setOption(final Option<T> option, final T value) throws IllegalArgumentException, IOException {
        if (option == Options.READ_TIMEOUT) {
            return option.cast(Integer.valueOf(conduit.getAndSetReadTimeout(Options.READ_TIMEOUT.cast(value).intValue())));
        } else if (option == Options.WRITE_TIMEOUT) {
            return option.cast(Integer.valueOf(conduit.getAndSetWriteTimeout(Options.WRITE_TIMEOUT.cast(value).intValue())));
        } else if (option == Options.SEND_BUFFER) {
            T old = option.cast(Integer.valueOf(Native.testAndThrow(Native.getOptSendBuffer(fd))));
            Native.testAndThrow(Native.setOptSendBuffer(fd, Options.SEND_BUFFER.cast(value).intValue()));
            return old;
        } else {
            return super.setOption(option, value);
        }
    }

    protected void setSourceConduit(final StreamSourceConduit conduit) {
        super.setSourceConduit(conduit);
    }

    protected void setSinkConduit(final StreamSinkConduit conduit) {
        super.setSinkConduit(conduit);
    }

    protected boolean readClosed() {
        return super.readClosed();
    }

    protected boolean writeClosed() {
        return super.writeClosed();
    }

    protected void notifyWriteClosed() {
        conduit.writeTerminated();
    }

    protected void notifyReadClosed() {
        conduit.readTerminated();
    }

    protected void closeAction() throws IOException {
        conduit.terminate();
    }

    public String toString() {
        return String.format("%s fd=%d id=%d", getClass().getName(), fd, conduit.id);
    }
}
