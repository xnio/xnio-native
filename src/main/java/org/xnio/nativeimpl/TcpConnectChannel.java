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
import org.jboss.logging.Logger;
import org.xnio.ChannelListener;
import org.xnio.ChannelListeners;
import org.xnio.FutureResult;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.channels.ConnectedStreamChannel;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class TcpConnectChannel extends NativeRawChannel<TcpConnectChannel> {

    private static final Logger log = Logger.getLogger("org.xnio.native.connect");

    private final FutureResult<ConnectedStreamChannel> futureResult;
    private final OptionMap optionMap;
    private final ChannelListener<? super ConnectedStreamChannel> openListener;
    private final NativeWorkerThread readThread;
    private final NativeWorkerThread writeThread;

    TcpConnectChannel(final NativeXnioWorker worker, final int fd, final FutureResult<ConnectedStreamChannel> result, final OptionMap map, final ChannelListener<? super ConnectedStreamChannel> listener, final NativeWorkerThread readThread, final NativeWorkerThread writeThread) {
        super(worker, fd);
        futureResult = result;
        optionMap = map;
        openListener = listener;
        this.readThread = readThread;
        this.writeThread = writeThread;
    }

    protected void notifyReady(final NativeWorkerThread thread) {
        log.tracef("Executing deferred connect on %s", this);
        int res = Native.finishConnect(fd);
        if (res == -Native.EAGAIN) {
            log.tracef("Deferred connect not ready on %s", this);
            return;
        }
        thread.resumeFd(fd, false, false);
        suppressFinalize();
        try {
            log.tracef("Deferred connect ready on %s", this);
            Native.testAndThrow(res);
        } catch (IOException e) {
            log.tracef(e, "Deferred connect failed on %s", this);
            thread.unregister(this);
            Native.close(fd);
            futureResult.setException(e);
            return;
        }
        final TcpSocketChannel channel;
        try {
            final NativeXnioWorker worker = getWorker();
            channel = new TcpSocketChannel(worker, fd, readThread, writeThread, null);
        } catch (IOException e) {
            log.tracef(e, "Deferred connect channel construction failed on %s", this);
            thread.unregister(this);
            Native.close(fd);
            futureResult.setException(e);
            return;
        }
        try {
            readThread.register(channel);
            writeThread.register(channel);
        } catch (IOException e) {
            log.tracef(e, "Deferred connect channel construction failed on %s", this);
            thread.unregister(this);
            readThread.unregister(channel);
            writeThread.unregister(channel);
            Native.close(fd);
            futureResult.setException(e);
            return;
        }
        log.tracef("Constructed deferred connect channel on %s: %s", this, channel);
        futureResult.setResult(channel);
        if (optionMap.get(Options.WORKER_ESTABLISH_WRITING, false)) {
            ChannelListeners.<ConnectedStreamChannel>invokeChannelListener(channel, openListener);
        } else {
            // not unsafe - http://youtrack.jetbrains.net/issue/IDEA-59290
            //noinspection unchecked
            channel.getReadThread().execute(ChannelListeners.getChannelListenerTask(channel, openListener));
        }
    }

    protected void unregisterAllHandles() {
    }
}
