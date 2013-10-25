/*
 * JBoss, Home of Professional Open Source
 *
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
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
import org.xnio.channels.ConnectedChannel;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
abstract class SocketChannel<C extends SocketChannel<C>> extends NativeSuspendableChannel<C> implements ConnectedChannel {
    private final SocketAddress localAddress;
    private final SocketAddress peerAddress;
    private final NativeAcceptChannel<?> acceptChannel;

    SocketChannel(final NativeXnioWorker worker, final int fd, final NativeWorkerThread readThread, final NativeWorkerThread writeThread, final NativeAcceptChannel<?> server) throws IOException {
        super(worker, fd, readThread, writeThread);
        acceptChannel = server;
        localAddress = Native.getSocketAddress(Native.getSockName(fd));
        peerAddress = Native.getSocketAddress(Native.getPeerName(fd));
    }

    public SocketAddress getPeerAddress() {
        return peerAddress;
    }

    public <A extends SocketAddress> A getPeerAddress(final Class<A> type) {
        final SocketAddress address = peerAddress;
        return type.isInstance(address) ? type.cast(address) : null;
    }

    public SocketAddress getLocalAddress() {
        return localAddress;
    }

    public <A extends SocketAddress> A getLocalAddress(final Class<A> type) {
        final SocketAddress address = localAddress;
        return type.isInstance(address) ? type.cast(address) : null;
    }

    private static final Set<Option<?>> OPTIONS = Option.setBuilder()
            .add(Options.SEND_BUFFER)
            .create();

    protected void firstClose() {
        final NativeAcceptChannel<?> acceptChannel = this.acceptChannel;
        if (acceptChannel != null) {
            acceptChannel.channelClosed();
        }
        super.firstClose();
    }

    public boolean supportsOption(final Option<?> option) {
        return OPTIONS.contains(option) || super.supportsOption(option);
    }

    public <T> T getOption(final Option<T> option) throws IOException {
        if (option == Options.SEND_BUFFER) {
            return getIntegerOption(option, Native.getOptSendBuffer(fd));
        } else {
            return super.getOption(option);
        }
    }

    public <T> T setOption(final Option<T> option, final T value) throws IllegalArgumentException, IOException {
        if (! isOpen()) {
            // fast path
            return null;
        }
        if (option == Options.SEND_BUFFER) {
            Native.testAndThrow(Native.setOptSendBuffer(fd, Options.SEND_BUFFER.cast(value).intValue()));
            return null;
        } else {
            return super.setOption(option, value);
        }
    }
}
