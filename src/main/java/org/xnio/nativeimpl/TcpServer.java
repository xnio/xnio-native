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
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.channels.AcceptingChannel;

final class TcpServer extends NativeAcceptChannel<TcpServer> implements AcceptingChannel<TcpSocketChannel> {

    TcpServer(final NativeXnioWorker worker, final int fd, final OptionMap optionMap) throws IOException {
        super(worker, fd, optionMap);
        if (optionMap.get(Options.REUSE_ADDRESSES, true)) {
            Native.testAndThrow(Native.setOptReuseAddr(fd, true));
        }
    }

    public TcpSocketChannel accept() throws IOException {
        final int sfd = doAccept();
        if (sfd < 0) {
            return null;
        }
        final NativeWorkerThread[] threads = getThreads();
        final NativeWorkerThread readThread;
        final NativeWorkerThread writeThread;
        final Thread currentThread = Thread.currentThread();
        if (threads.length > 1 && (currentThread instanceof NativeWorkerThread)) {
            NativeWorkerThread nativeWorkerThread = (NativeWorkerThread) currentThread;
            if (nativeWorkerThread.isWriteThread()) {
                writeThread = nativeWorkerThread;
                readThread = getWorker().choose(false);
            } else {
                writeThread = getWorker().choose(true);
                readThread = nativeWorkerThread;
            }
        } else {
            readThread = getWorker().choose(false);
            writeThread = getWorker().choose(true);
        }
        final TcpSocketChannel channel = new TcpSocketChannel(getWorker(), sfd, readThread, writeThread, null);
        readThread.register(channel);
        writeThread.register(channel);
        return channel;
    }
}
