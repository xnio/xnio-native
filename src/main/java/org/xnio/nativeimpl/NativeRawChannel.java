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
import java.nio.channels.Channel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.jboss.logging.Logger;
import org.xnio.ChannelListener;
import org.xnio.ChannelListeners;
import org.xnio.Option;
import org.xnio.channels.CloseableChannel;

import static org.xnio.Bits.*;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
abstract class NativeRawChannel<C extends NativeRawChannel<C>> implements Channel, CloseableChannel {

    private static final Logger log = Logger.getLogger("org.xnio.native");

    private static final int[] NO_WAITERS = new int[0];

    private final NativeXnioWorker worker;

    private volatile ChannelListener<? super C> closeListener;
    private volatile int[] waiters = NO_WAITERS;
    @SuppressWarnings("unused")
    private volatile int state;

    final int fd;

    private static final int CLOSE_READ_FLAG  = 1 << 0;
    private static final int CLOSE_WRITE_FLAG = 1 << 1;
    private static final int RESUME_READ_FLAG = 1 << 2;
    private static final int RESUME_WRITE_FLAG = 1 << 3;
    private static final int IN_SUSPEND_RESUME_READ = 1 << 4;
    private static final int IN_SUSPEND_RESUME_WRITE = 1 << 5;
    private static final int SUPPRESS_FINALIZE = 1 << 6;

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<NativeRawChannel> stateUpdater = AtomicIntegerFieldUpdater.newUpdater(NativeRawChannel.class, "state");
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<NativeRawChannel, int[]> waitersUpdater = AtomicReferenceFieldUpdater.newUpdater(NativeRawChannel.class, int[].class, "waiters");

    protected NativeRawChannel(final NativeXnioWorker worker, final int fd) {
        this.worker = worker;
        this.fd = fd;
    }

    protected abstract void notifyReady(NativeWorkerThread thread);

    @SuppressWarnings("unchecked")
    final C getTyped() {
        return (C) this;
    }

    public final ChannelListener.Setter<C> getCloseSetter() {
        return new ChannelListener.Setter<C>() {
            public void set(final ChannelListener<? super C> listener) {
                closeListener = listener;
            }
        };
    }

    final void invokeCloseListener() {
        ChannelListeners.invokeChannelListener(getTyped(), closeListener);
    }

    public boolean isOpen() {
        return anyAreClear(state, CLOSE_READ_FLAG | CLOSE_WRITE_FLAG);
    }

    private boolean casState(int expect, int update) {
        return stateUpdater.compareAndSet(this, expect, update);
    }

    protected final int closeReads() {
        int oldVal, newVal;
        do {
            oldVal = state;
            if (allAreSet(oldVal, CLOSE_READ_FLAG | CLOSE_WRITE_FLAG)) {
                return 0;
            }
            newVal = oldVal & ~RESUME_READ_FLAG | CLOSE_READ_FLAG;
        } while (! casState(oldVal, newVal));
        return allAreSet(oldVal, CLOSE_WRITE_FLAG) ? 2 : 1;
    }

    protected final int closeWrites() {
        int oldVal, newVal;
        do {
            oldVal = state;
            if (allAreSet(oldVal, CLOSE_READ_FLAG | CLOSE_WRITE_FLAG)) {
                return 0;
            }
            newVal = oldVal & ~RESUME_WRITE_FLAG | CLOSE_WRITE_FLAG;
        } while (! casState(oldVal, newVal));
        return allAreSet(oldVal, CLOSE_READ_FLAG) ? 2 : 1;
    }

    protected final boolean readsResumed() {
        return allAreSet(state, RESUME_READ_FLAG);
    }

    protected final boolean writesResumed() {
        return allAreSet(state, RESUME_WRITE_FLAG);
    }

    protected final boolean setReadsResumed() {
        int oldVal, newVal;
        do {
            oldVal = state;
            if (anyAreSet(oldVal, RESUME_READ_FLAG | CLOSE_READ_FLAG | IN_SUSPEND_RESUME_READ)) {
                return false;
            }
            newVal = oldVal | RESUME_READ_FLAG | IN_SUSPEND_RESUME_READ;
        } while (! casState(oldVal, newVal));
        return true;
    }

    protected final boolean clearReadsResumed() {
        int oldVal, newVal;
        do {
            oldVal = state;
            if (anyAreSet(oldVal, CLOSE_READ_FLAG | IN_SUSPEND_RESUME_READ) || allAreClear(oldVal, RESUME_READ_FLAG)) {
                return false;
            }
            newVal = oldVal & ~RESUME_READ_FLAG | IN_SUSPEND_RESUME_READ;
        } while (! casState(oldVal, newVal));
        return true;
    }

    protected final void exitReadsResumed() {
        stateUpdater.getAndAdd(this, -IN_SUSPEND_RESUME_READ);
    }

    protected final boolean setWritesResumed() {
        int oldVal, newVal;
        do {
            oldVal = state;
            if (anyAreSet(oldVal, RESUME_WRITE_FLAG | CLOSE_WRITE_FLAG | IN_SUSPEND_RESUME_WRITE)) {
                return false;
            }
            newVal = oldVal | RESUME_WRITE_FLAG | IN_SUSPEND_RESUME_WRITE;
        } while (! casState(oldVal, newVal));
        return true;
    }

    protected final boolean clearWritesResumed() {
        int oldVal, newVal;
        do {
            oldVal = state;
            if (anyAreSet(oldVal, CLOSE_WRITE_FLAG | IN_SUSPEND_RESUME_WRITE) || allAreClear(oldVal, RESUME_WRITE_FLAG)) {
                return false;
            }
            newVal = oldVal & ~RESUME_WRITE_FLAG | IN_SUSPEND_RESUME_WRITE;
        } while (! casState(oldVal, newVal));
        return true;
    }

    protected final void exitWritesResumed() {
        stateUpdater.getAndAdd(this, -IN_SUSPEND_RESUME_WRITE);
    }

    public final void close() throws IOException {
        int oldVal, newVal;
        do {
            oldVal = state;
            if (allAreSet(oldVal, CLOSE_READ_FLAG | CLOSE_WRITE_FLAG)) {
                log.tracef("Idempotent close of %s", this);
                return;
            }
            newVal = oldVal | CLOSE_READ_FLAG | CLOSE_WRITE_FLAG;
        } while (! casState(oldVal, newVal));
        log.tracef("Closing of %s", this);
        wakeAll();
        try {
            closeAction();
        } finally {
            unregisterAllHandles();
            invokeCloseListener();
            log.tracef("Closing of %s complete", this);
        }
    }

    protected final void wakeAll() {
        final int[] waiters = waitersUpdater.getAndSet(this, NO_WAITERS);
        // wake up all waiters
        for (int waiter : waiters) {
            Native.writeLong(waiter, 1L);
        }
    }

    /**
     * Add a waiting thread for this channel.  Must be followed by a try/finally with a removeWaiter in the finally,
     * otherwise FD corruption can occur.
     *
     * @param threadFd
     * @param writes
     */
    protected final boolean addWaiter(int threadFd, final boolean writes) {
        int[] oldVal, newVal;
        int oldLen;
        do {
            if (allAreSet(state, writes ? CLOSE_WRITE_FLAG : CLOSE_READ_FLAG)) {
                return false;
            }
            oldVal = waiters;
            oldLen = oldVal.length;
            newVal = Arrays.copyOf(oldVal, oldLen + 1);
            newVal[oldLen] = threadFd;
        } while (! waitersUpdater.compareAndSet(this, oldVal, newVal));
        if (allAreSet(state, writes ? CLOSE_WRITE_FLAG : CLOSE_READ_FLAG)) {
            wakeAll();
            return false;
        }
        return true;
    }

    protected final void removeWaiter(int threadFd) {
        int[] oldVal, newVal;
        int oldLen;
        do {
            oldVal = waiters;
            oldLen = oldVal.length;
            if (oldLen == 0) {
                return;
            } else if (oldLen == 1) {
                assert oldVal[0] == threadFd;
                newVal = NO_WAITERS;
            } else {
                newVal = Arrays.copyOf(oldVal, oldLen - 1);
                if (oldVal[oldLen - 1] != threadFd) {
                    for (int idx = 0; idx < oldLen - 1; idx++) {
                        if (oldVal[idx] == threadFd) {
                            newVal[idx] = oldVal[oldLen - 1];
                            break;
                        }
                    }
                }
            }
        } while (! waitersUpdater.compareAndSet(this, oldVal, newVal));
    }

    protected abstract void unregisterAllHandles();

    protected void closeAction() throws IOException {
        Native.testAndThrow(Native.dup2(Native.DEAD_FD, fd));
    }

    public String toString() {
        StringBuilder b = new StringBuilder(20);
        b.append("filedes=").append(fd);
        int state = this.state;
        if (allAreSet(state, CLOSE_READ_FLAG | CLOSE_WRITE_FLAG)) {
            b.append(" (closed)");
        } else if (allAreSet(state, CLOSE_READ_FLAG)) {
            b.append(" (reads shutdown)");
        } else if (allAreSet(state, CLOSE_WRITE_FLAG)) {
            b.append(" (writes shutdown)");
        }
        return b.toString();
    }

    public boolean supportsOption(final Option<?> option) {
        return false;
    }

    public <T> T getOption(final Option<T> option) throws IOException {
        return null;
    }

    public <T> T setOption(final Option<T> option, final T value) throws IllegalArgumentException, IOException {
        return null;
    }

    static <T> T getBooleanOption(Option<T> option, int res) throws IOException {
        if (res < 0) {
            throw Native.exceptionFor(res);
        }
        return option.cast(Boolean.valueOf(res != 0));
    }

    static <T> T getIntegerOption(Option<T> option, int res) throws IOException {
        if (res < 0) {
            throw Native.exceptionFor(res);
        }
        return option.cast(Integer.valueOf(res));
    }

    public NativeXnioWorker getWorker() {
        return worker;
    }

    void suppressFinalize() {
        int s;
        do {
            s = state;
        } while (! casState(s, s | SUPPRESS_FINALIZE));
    }

    protected void finalize() throws Throwable {
        if (allAreClear(state, SUPPRESS_FINALIZE)) {
            log.tracef("Finalize of %s", this);
            Native.close(fd);
        } else {
            log.tracef("Finalize of %s suppressed", this);
        }
    }
}
