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
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;
import org.xnio.ChannelListener;
import org.xnio.ChannelListeners;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
abstract class NativeSuspendableChannel<C extends NativeSuspendableChannel<C>> extends NativeRawChannel<C> {
    private static final Logger log = Logger.getLogger("org.xnio.native");
    private static final String FQCN = NativeSuspendableChannel.class.getName();

    private volatile ChannelListener<? super C> readListener;
    private volatile ChannelListener<? super C> writeListener;
    private final NativeWorkerThread readThread;
    private final NativeWorkerThread writeThread;

    NativeSuspendableChannel(final NativeXnioWorker worker, final int fd, final NativeWorkerThread readThread, final NativeWorkerThread writeThread) throws IOException {
        super(worker, fd);
        this.readThread = readThread;
        this.writeThread = writeThread;
    }

    protected void notifyReady(final NativeWorkerThread thread) {
        if (thread == readThread) {
            invokeReadListener();
        } else {
            invokeWriteListener();
        }
    }

    public ChannelListener.Setter<? extends C> getReadSetter() {
        return new ChannelListener.Setter<C>() {
            public void set(final ChannelListener<? super C> listener) {
                readListener = listener;
            }
        };
    }

    public ChannelListener.Setter<? extends C> getWriteSetter() {
        return new ChannelListener.Setter<C>() {
            public void set(final ChannelListener<? super C> listener) {
                writeListener = listener;
            }
        };
    }

    void invokeWriteListener() {
        if (writesResumed()) {
            ChannelListeners.invokeChannelListener(getTyped(), writeListener);
        }
    }

    void invokeReadListener() {
        if (readsResumed()) {
            ChannelListeners.invokeChannelListener(getTyped(), readListener);
        }
    }

    public final boolean isReadResumed() {
        return readsResumed();
    }

    public final boolean isWriteResumed() {
        return writesResumed();
    }

    protected void firstClose() {
        invokeCloseListener();
    }

    public void shutdownReads() throws IOException {
        switch (closeReads()) {
            case 2: {
                invokeCloseListener();
                wakeAll();
            }
            case 1: {
                readThread.unregister(this);
                if (log.isTraceEnabled()) {
                    log.logf(FQCN, Logger.Level.TRACE, null, "Shutting down reads on %s", this);
                }
            }
            case 0: break;
        }
    }

    public void shutdownWrites() throws IOException {
        switch (closeWrites()) {
            case 2: {
                invokeCloseListener();
                wakeAll();
            }
            case 1: {
                writeThread.unregister(this);
                if (log.isTraceEnabled()) {
                    log.logf(FQCN, Logger.Level.TRACE, null, "Shutting down writes on %s", this);
                }
            }
            case 0: break;
        }
    }

    protected void unregisterAllHandles() {
        readThread.unregister(this);
        writeThread.unregister(this);
    }

    public void awaitReadable() throws IOException {
        final int fd = ThreadFd.get().fd;
        if (addWaiter(fd, false)) try {
            Native.testAndThrow(Native.await2(fd, false));
        } finally {
            removeWaiter(fd);
        }
    }

    public void awaitReadable(final long time, final TimeUnit timeUnit) throws IOException {
        final int fd = ThreadFd.get().fd;
        if (addWaiter(fd, false)) try {
            Native.testAndThrow(Native.await3(fd, false, timeUnit.toMillis(time)));
        } finally {
            removeWaiter(fd);
        }
    }

    public void awaitWritable() throws IOException {
        final int fd = ThreadFd.get().fd;
        if (addWaiter(fd, true)) try {
            Native.testAndThrow(Native.await2(fd, true));
        } finally {
            removeWaiter(fd);
        }
    }

    public void awaitWritable(final long time, final TimeUnit timeUnit) throws IOException {
        final int fd = ThreadFd.get().fd;
        if (addWaiter(fd, true)) try {
            Native.testAndThrow(Native.await3(fd, true, timeUnit.toMillis(time)));
        } finally {
            removeWaiter(fd);
        }
    }

    public final void suspendReads() {
        if (clearReadsResumed()) try {
            log.logf(FQCN, Logger.Level.TRACE, null, "Suspending reads on %s", this);
            readThread.resumeFd(fd, false, false);
        } finally {
            exitReadsResumed();
        }
    }

    public final void suspendWrites() {
        if (clearWritesResumed()) try {
            log.logf(FQCN, Logger.Level.TRACE, null, "Suspending writes on %s", this);
            writeThread.resumeFd(fd, false, false);
        } finally {
            exitWritesResumed();
        }
    }

    public final void resumeReads() {
        if (setReadsResumed()) try {
            log.logf(FQCN, Logger.Level.TRACE, null, "Resuming reads on %s", this);
            readThread.resumeFd(fd, true, false);
        } finally {
            exitReadsResumed();
        }
    }

    public final void resumeWrites() {
        if (setWritesResumed()) try {
            log.logf(FQCN, Logger.Level.TRACE, null, "Resuming writes on %s", this);
            writeThread.resumeFd(fd, false, true);
        } finally {
            exitWritesResumed();
        }
    }

    public final void wakeupReads() {
        if (setReadsResumed()) try {
            log.logf(FQCN, Logger.Level.TRACE, null, "Wakeup reads on %s", this);
            readThread.resumeFd(fd, true, false);
        } finally {
            exitReadsResumed();
        }
        readThread.execute(new Runnable() {
            public void run() {
                ChannelListeners.invokeChannelListener(getTyped(), readListener);
            }
        });
    }

    public final void wakeupWrites() {
        if (setWritesResumed()) try {
            log.logf(FQCN, Logger.Level.TRACE, null, "Wakeup writes on %s", this);
            writeThread.resumeFd(fd, false, true);
        } finally {
            exitWritesResumed();
        }
        writeThread.execute(new Runnable() {
            public void run() {
                ChannelListeners.invokeChannelListener(getTyped(), writeListener);
            }
        });
    }

    public NativeWorkerThread getReadThread() {
        return readThread;
    }

    public NativeWorkerThread getWriteThread() {
        return writeThread;
    }

    public boolean flush() throws IOException {
        return true;
    }
}
