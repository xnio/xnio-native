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

import static org.xnio.nativeimpl.Log.log;

import java.io.IOException;
import java.util.Set;

import org.xnio.Option;
import org.xnio.OptionMap;
import org.xnio.StreamConnection;
import org.xnio.XnioExecutor;
import org.xnio.XnioIoThread;
import org.xnio.channels.AcceptingChannel;
import org.xnio.channels.UnsupportedOptionException;

final class UnixServer extends NativeAcceptChannel<UnixServer> implements AcceptingChannel<StreamConnection> {

    private static final Set<Option<?>> options = Option.setBuilder()
            .create();

    UnixServer(final NativeXnioWorker worker, final int fd, final OptionMap optionMap) throws IOException {
        super(worker, fd, optionMap);
        final NativeWorkerThread[] threads = worker.getAll();
        final int threadCount = threads.length;
        if (threadCount == 0) {
            throw log.noThreads();
        }
    }

    public XnioExecutor getAcceptThread() {
        return null;
    }

    public XnioIoThread getIoThread() {
        return null;
    }

    protected NativeStreamConnection constructConnection(int fd, NativeWorkerThread thread) {
        return new UnixConnection(thread, fd);
    }

    public boolean supportsOption(final Option<?> option) {
        return options.contains(option) || super.supportsOption(option);
    }

    public <T> T getOption(final Option<T> option) throws UnsupportedOptionException, IOException {
        return super.getOption(option);
    }

    public <T> T setOption(final Option<T> option, final T value) throws IllegalArgumentException, IOException {
        return super.setOption(option, value);
    }
}
