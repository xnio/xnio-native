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
import java.util.Set;
import org.xnio.Option;
import org.xnio.Options;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class TcpSocketChannel extends StreamSocketChannel<TcpSocketChannel> {

    TcpSocketChannel(final NativeXnioWorker worker, final int fd, final NativeWorkerThread readThread, final NativeWorkerThread writeThread, final NativeAcceptChannel<?> acceptChannel) throws IOException {
        super(worker, fd, readThread, writeThread, acceptChannel);
        Native.testAndThrow(Native.setOptTcpNoDelay(fd, true));
        Native.testAndThrow(Native.setOptTcpCork(fd, true));
    }

    private static final Set<Option<?>> OPTIONS = Option.setBuilder()
            .add(Options.KEEP_ALIVE)
            .add(Options.RECEIVE_BUFFER)
            .create();

    public boolean supportsOption(final Option<?> option) {
        return OPTIONS.contains(option) || super.supportsOption(option);
    }

    public <T> T getOption(final Option<T> option) throws IOException {
        if (option == Options.KEEP_ALIVE) {
            return getBooleanOption(option, Native.getOptKeepAlive(fd));
        } else if (option == Options.RECEIVE_BUFFER) {
            return getIntegerOption(option, Native.getOptReceiveBuffer(fd));
        } else {
            return super.getOption(option);
        }
    }

    public <T> T setOption(final Option<T> option, final T value) throws IllegalArgumentException, IOException {
        if (! isOpen()) {
            // fast path
            return null;
        }
        if (option == Options.KEEP_ALIVE) {
            Native.testAndThrow(Native.setOptKeepAlive(fd, Options.KEEP_ALIVE.cast(value).booleanValue()));
            return null;
        } else if (option == Options.RECEIVE_BUFFER) {
            Native.testAndThrow(Native.setOptReceiveBuffer(fd, Options.RECEIVE_BUFFER.cast(value).intValue()));
            return null;
        } else {
            return super.setOption(option, value);
        }
    }

    public boolean flush() throws IOException {
        Native.testAndThrow(Native.setOptTcpCork(fd, false));
        Native.testAndThrow(Native.setOptTcpCork(fd, true));
        Native.log.tracef("Flushed %s", this);
        return true;
    }
}
