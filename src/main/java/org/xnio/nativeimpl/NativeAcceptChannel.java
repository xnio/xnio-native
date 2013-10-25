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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.jboss.logging.Logger;
import org.xnio.ChannelListener;
import org.xnio.ChannelListeners;
import org.xnio.Option;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.channels.SuspendableAcceptChannel;
import org.xnio.channels.UnsupportedOptionException;

import static java.util.concurrent.locks.LockSupport.unpark;
import static org.xnio.Bits.allAreClear;
import static org.xnio.Bits.allAreSet;
import static org.xnio.Bits.longBitMask;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
abstract class NativeAcceptChannel<C extends NativeAcceptChannel<C>> extends NativeRawChannel<C> implements SuspendableAcceptChannel {
    private static final Logger log = Logger.getLogger("org.xnio.native");

    private volatile ChannelListener<? super C> acceptListener;

    private final SocketAddress localAddress;
    private final NativeWorkerThread[] threads;

    private volatile long connectionStatus = CONN_LOW_MASK | CONN_HIGH_MASK;

    private static final long CONN_COUNT_MASK   = longBitMask(0, 19);
    private static final long CONN_COUNT_BIT    = 0L;
    private static final long CONN_COUNT_ONE    = 1L << CONN_COUNT_BIT;
    private static final long CONN_LOW_MASK     = longBitMask(20, 39);
    private static final long CONN_LOW_BIT      = 20L;
    @SuppressWarnings("unused")
    private static final long CONN_LOW_ONE      = 1L << CONN_LOW_BIT;
    private static final long CONN_HIGH_MASK    = longBitMask(40, 59);
    private static final long CONN_HIGH_BIT     = 40L;
    @SuppressWarnings("unused")
    private static final long CONN_HIGH_ONE     = 1L << CONN_HIGH_BIT;
    private static final long CONN_SUSPENDING   = 1L << 60L;
    private static final long CONN_FULL         = 1L << 61L;
    private static final long CONN_RESUMED      = 1L << 62L;

    @SuppressWarnings("unused")
    private volatile Thread waitingThread;

    @SuppressWarnings("rawtypes")
    private static final AtomicLongFieldUpdater<NativeAcceptChannel> connectionStatusUpdater = AtomicLongFieldUpdater.newUpdater(NativeAcceptChannel.class, "connectionStatus");
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<NativeAcceptChannel, Thread> waitingThreadUpdater = AtomicReferenceFieldUpdater.newUpdater(NativeAcceptChannel.class, Thread.class, "waitingThread");

    NativeAcceptChannel(final NativeXnioWorker worker, final int fd, final OptionMap optionMap) throws IOException {
        super(worker, fd);
        localAddress = Native.getSocketAddress(Native.getSockName(fd));
        final boolean write = optionMap.get(Options.WORKER_ESTABLISH_WRITING, false);
        final int count = optionMap.get(Options.WORKER_ACCEPT_THREADS, 1);
        final NativeWorkerThread[] threads = worker.choose(count, write);
        for (NativeWorkerThread thread : threads) {
            thread.register(this);
        }
        this.threads = threads;
    }

    protected void notifyReady(final NativeWorkerThread thread) {
        invokeAcceptListener();
    }

    public ChannelListener.Setter<? extends C> getAcceptSetter() {
        return new ChannelListener.Setter<C>() {
            public void set(final ChannelListener<? super C> listener) {
                acceptListener = listener;
            }
        };
    }

    void invokeAcceptListener() {
        if (readsResumed()) {
            ChannelListeners.invokeChannelListener(getTyped(), acceptListener);
        }
    }

    NativeWorkerThread[] getThreads() {
        return threads;
    }

    protected final int doAccept() throws IOException {
        // This method changes the state of the CONN_SUSPENDING flag.
        // As such it is responsible to make sure that when the flag is cleared, the resume state accurately
        // reflects the state of the CONN_RESUMED and CONN_FULL flags.
        long oldVal, newVal;
        do {
            oldVal = connectionStatus;
            if (allAreSet(oldVal, CONN_FULL)) {
                log.trace("No connection accepted (full)");
                return -1;
            }
            newVal = oldVal + CONN_COUNT_ONE;
            if (getCount(newVal) >= getHighWater(newVal)) {
                newVal |= CONN_SUSPENDING | CONN_FULL;
            }
        } while (! connectionStatusUpdater.compareAndSet(this, oldVal, newVal));
        boolean wasSuspended = allAreClear(oldVal, CONN_RESUMED);
        boolean doSuspend = ! wasSuspended && allAreClear(oldVal, CONN_SUSPENDING) && allAreSet(newVal, CONN_FULL | CONN_SUSPENDING);

        final int sfd;
        sfd = Native.accept(fd);
        if (sfd == -Native.EAGAIN) {
            log.trace("No connection accepted");
            return -1;
        }
        try {
            return Native.testAndThrow(sfd);
        } catch (IOException e) {
            undoAccept(newVal, wasSuspended, doSuspend);
            log.tracef("No connection accepted (%s)", e);
            return -1;
        }
    }

    private long updateWaterMark(int reqNewLowWater, int reqNewHighWater) {
        // at least one must be specified
        assert reqNewLowWater != -1 || reqNewHighWater != -1;
        // if both given, low must be less than high
        assert reqNewLowWater == -1 || reqNewHighWater == -1 || reqNewLowWater <= reqNewHighWater;
        long oldVal, newVal;
        int oldHighWater, oldLowWater, connCount;
        int newLowWater, newHighWater;
        do {
            oldVal = connectionStatus;
            oldLowWater = getLowWater(oldVal);
            oldHighWater = getHighWater(oldVal);
            connCount = getCount(oldVal);
            newLowWater = reqNewLowWater == -1 ? oldLowWater : reqNewLowWater;
            newHighWater = reqNewHighWater == -1 ? oldHighWater : reqNewHighWater;
            // Make sure the new values make sense
            if (reqNewLowWater != -1 && newLowWater > newHighWater) {
                newHighWater = newLowWater;
            } else if (reqNewHighWater != -1 && newHighWater < newLowWater) {
                newLowWater = newHighWater;
            }
            // See if the change would be redundant
            if (oldLowWater == newLowWater && oldHighWater == newHighWater) {
                return oldVal;
            }
            newVal = withLowWater(withHighWater(oldVal, newHighWater), newLowWater);
            // determine if we need to suspend because the high water line dropped below count
            //    ...or if we need to resume because the low water line rose above count
            if (allAreClear(oldVal, CONN_FULL) && oldHighWater > connCount && newHighWater <= connCount) {
                newVal |= CONN_FULL | CONN_SUSPENDING;
            } else if (allAreSet(oldVal, CONN_FULL) && oldLowWater < connCount && newLowWater >= connCount) {
                newVal &= ~CONN_FULL;
                newVal |= CONN_SUSPENDING;
            }
        } while (! connectionStatusUpdater.compareAndSet(this, oldVal, newVal));
        if (allAreSet(oldVal, CONN_FULL) && allAreClear(newVal, CONN_FULL)) {
            final Thread thread = waitingThreadUpdater.getAndSet(this, null);
            if (thread != null) {
                unpark(thread);
            }
        }
        if (allAreClear(oldVal, CONN_SUSPENDING) && allAreSet(newVal, CONN_SUSPENDING)) {
            // we have work to do...
            if (allAreSet(newVal, CONN_FULL)) {
                for (NativeWorkerThread thread : getThreads()) {
                    thread.resumeFd(fd, false, false);
                }
                synchronizeConnectionState(newVal, true);
            } else {
                for (NativeWorkerThread thread : getThreads()) {
                    thread.resumeFd(fd, true, false);
                }
                synchronizeConnectionState(newVal, false);
            }
        }
        return oldVal;
    }

    private static int getHighWater(final long value) {
        return (int) ((value & CONN_HIGH_MASK) >> CONN_HIGH_BIT);
    }

    private static int getLowWater(final long value) {
        return (int) ((value & CONN_LOW_MASK) >> CONN_LOW_BIT);
    }

    private static int getCount(final long value) {
        return (int) ((value & CONN_COUNT_MASK) >> CONN_COUNT_BIT);
    }

    private static long withHighWater(final long oldValue, final int highWater) {
        return oldValue & ~CONN_HIGH_MASK | (long)highWater << CONN_HIGH_BIT;
    }

    private static long withLowWater(final long oldValue, final int lowWater) {
        return oldValue & ~CONN_LOW_MASK | (long)lowWater << CONN_LOW_BIT;
    }

    private void synchronizeConnectionState(long oldVal, boolean suspended) {
        long newVal;
        newVal = oldVal & ~CONN_SUSPENDING;
        while (!connectionStatusUpdater.compareAndSet(this, oldVal, newVal)) {
            oldVal = connectionStatus;
            // it's up to whoever increments or decrements connectionStatus to set or clear CONN_FULL
            if ((allAreClear(oldVal, CONN_FULL) && allAreSet(oldVal, CONN_RESUMED)) != suspended) {
                for (NativeWorkerThread thread : getThreads()) {
                    thread.resumeFd(fd, !(suspended = !suspended), false);
                }
            }
            newVal = oldVal & ~CONN_SUSPENDING;
        }
    }

    private void undoAccept(long newVal, final boolean wasSuspended, boolean doSuspend) {
        // re-synchronize the resume status of this channel
        // first assume that the value hasn't changed
        long oldVal = newVal;
        newVal = oldVal - CONN_COUNT_ONE;
        newVal &= ~(CONN_FULL | CONN_SUSPENDING);
        doSuspend = !doSuspend && !wasSuspended;
        while (! connectionStatusUpdater.compareAndSet(this, oldVal, newVal)) {
            // the value has changed - reevaluate everything necessary to resynchronize resume and decrement the count
            oldVal = connectionStatus;
            newVal = (oldVal - CONN_COUNT_ONE) & ~CONN_SUSPENDING;
            if (allAreSet(newVal, CONN_FULL) && (newVal & CONN_COUNT_MASK) >> CONN_COUNT_BIT <= (newVal & CONN_LOW_MASK) >> CONN_LOW_BIT) {
                // dropped below the line
                newVal &= ~CONN_FULL;
            }
            if ((allAreClear(newVal, CONN_FULL) && allAreSet(newVal, CONN_RESUMED)) != doSuspend) {
                for (NativeWorkerThread thread : getThreads()) {
                    thread.resumeFd(fd, !(doSuspend = !doSuspend), false);
                }
            }
        }
        if (allAreSet(oldVal, CONN_FULL) && allAreClear(newVal, CONN_FULL)) {
            final Thread thread = waitingThreadUpdater.getAndSet(this, null);
            if (thread != null) {
                unpark(thread);
            }
        }
    }

    void channelClosed() {
        long oldVal, newVal;
        do {
            oldVal = connectionStatus;
            newVal = oldVal - CONN_COUNT_ONE;
            if (allAreSet(newVal, CONN_FULL) && (newVal & CONN_COUNT_MASK) >> CONN_COUNT_BIT <= (newVal & CONN_LOW_MASK) >> CONN_LOW_BIT) {
                // dropped below the line
                newVal &= ~CONN_FULL;
                if (allAreSet(newVal, CONN_RESUMED)) {
                    newVal |= CONN_SUSPENDING;
                }
            }
        } while (! connectionStatusUpdater.compareAndSet(this, oldVal, newVal));
        if (allAreSet(oldVal, CONN_FULL) && allAreClear(newVal, CONN_FULL)) {
            final Thread thread = waitingThreadUpdater.getAndSet(this, null);
            if (thread != null) {
                unpark(thread);
            }
        }
        if (allAreSet(oldVal, CONN_SUSPENDING) || allAreClear(newVal, CONN_SUSPENDING)) {
            // done - we either didn't change the full setting, or we did but someone already has the suspending status,
            // or the user doesn't want to resume anyway, so we don't need to do anything about it
            return;
        }
        // We attempt to resume at this point.
        boolean doSuspend = false;
        for (NativeWorkerThread thread : getThreads()) {
            thread.resumeFd(fd, true, false);
        }
        oldVal = newVal;
        newVal &= ~CONN_SUSPENDING;
        while (! connectionStatusUpdater.compareAndSet(this, oldVal, newVal)) {
            // the value has changed - reevaluate everything necessary to resynchronize resume and decrement the count
            oldVal = connectionStatus;
            newVal = (oldVal - CONN_COUNT_ONE) & ~CONN_SUSPENDING;
            if (allAreSet(newVal, CONN_FULL) && (newVal & CONN_COUNT_MASK) >> CONN_COUNT_BIT <= (newVal & CONN_LOW_MASK) >> CONN_LOW_BIT) {
                // dropped below the line
                newVal &= ~CONN_FULL;
            }
            if ((allAreClear(newVal, CONN_FULL) && allAreSet(newVal, CONN_RESUMED)) != doSuspend) {
                for (NativeWorkerThread thread : getThreads()) {
                    thread.resumeFd(fd, !(doSuspend = !doSuspend), false);
                }
            }
        }
    }

    protected void unregisterAllHandles() {
        for (NativeWorkerThread thread : threads) {
            thread.unregister(this);
        }
    }

    protected void firstClose() {
        invokeCloseListener();
    }

    public void awaitAcceptable() throws IOException {
        final int fd = ThreadFd.get().fd;
        if (addWaiter(fd, false)) try {
            Native.testAndThrow(Native.await2(fd, false));
        } finally {
            removeWaiter(fd);
        }
    }

    public void awaitAcceptable(final long time, final TimeUnit timeUnit) throws IOException {
        final int fd = ThreadFd.get().fd;
        if (addWaiter(fd, false)) try {
            Native.testAndThrow(Native.await3(fd, false, timeUnit.toMillis(time)));
        } finally {
            removeWaiter(fd);
        }
    }

    public void suspendAccepts() {
        if (clearReadsResumed()) try {
            for (NativeWorkerThread thread : threads) {
                thread.resumeFd(fd, false, false);
            }
        } finally {
            exitReadsResumed();
        }
    }

    public void resumeAccepts() {
        if (setReadsResumed()) try {
            for (NativeWorkerThread thread : threads) {
                thread.resumeFd(fd, true, false);
            }
        } finally {
            exitReadsResumed();
        }
    }

    public void wakeupAccepts() {
        if (setReadsResumed()) try {
            for (NativeWorkerThread thread : threads) {
                thread.resumeFd(fd, true, false);
            }
        } finally {
            exitReadsResumed();
        }
        threads[0].execute(new Runnable() {
            public void run() {
                ChannelListeners.invokeChannelListener(getTyped(), acceptListener);
            }
        });
    }

    void setAcceptListener(final ChannelListener<? super C> acceptListener) {
        this.acceptListener = acceptListener;
    }

    public SocketAddress getLocalAddress() {
        return localAddress;
    }

    public <A extends SocketAddress> A getLocalAddress(final Class<A> type) {
        final SocketAddress address = getLocalAddress();
        return type.isInstance(address) ? type.cast(address) : null;
    }

    private static final Set<Option<?>> options = Option.setBuilder()
            .add(Options.REUSE_ADDRESSES)
            .add(Options.RECEIVE_BUFFER)
            .add(Options.KEEP_ALIVE)
            .add(Options.CONNECTION_HIGH_WATER)
            .add(Options.CONNECTION_LOW_WATER)
            .create();

    public boolean supportsOption(final Option<?> option) {
        return options.contains(option);
    }

    public <T> T getOption(final Option<T> option) throws UnsupportedOptionException, IOException {
        if (option == Options.REUSE_ADDRESSES) {
            return option.cast(Boolean.valueOf(Native.testAndThrow(Native.getOptReuseAddr(fd)) != 0));
        } else if (option == Options.RECEIVE_BUFFER) {
            return option.cast(Integer.valueOf(Native.testAndThrow(Native.getOptReceiveBuffer(fd))));
        } else if (option == Options.KEEP_ALIVE) {
            return option.cast(Boolean.valueOf(Native.testAndThrow(Native.getOptKeepAlive(fd)) != 0));
        } else if (option == Options.CONNECTION_HIGH_WATER) {
            return option.cast(Integer.valueOf(getHighWater(connectionStatus)));
        } else if (option == Options.CONNECTION_LOW_WATER) {
            return option.cast(Integer.valueOf(getLowWater(connectionStatus)));
        } else {
            return super.getOption(option);
        }
    }

    public <T> T setOption(final Option<T> option, final T value) throws IllegalArgumentException, IOException {
        final T old;
        if (option == Options.REUSE_ADDRESSES) {
            old = option.cast(Boolean.valueOf(Native.testAndThrow(Native.getOptReuseAddr(fd)) != 0));
            Native.testAndThrow(Native.setOptReuseAddr(fd, Options.REUSE_ADDRESSES.cast(value).booleanValue()));
            return old;
        } else if (option == Options.RECEIVE_BUFFER) {
            old = option.cast(Boolean.valueOf(Native.testAndThrow(Native.getOptReceiveBuffer(fd)) != 0));
            Native.testAndThrow(Native.setOptReceiveBuffer(fd, Options.RECEIVE_BUFFER.cast(value).intValue()));
            return old;
        } else if (option == Options.KEEP_ALIVE) {
            old = option.cast(Boolean.valueOf(Native.testAndThrow(Native.getOptKeepAlive(fd)) != 0));
            Native.testAndThrow(Native.setOptKeepAlive(fd, Options.REUSE_ADDRESSES.cast(value).booleanValue()));
            return old;
        } else if (option == Options.CONNECTION_HIGH_WATER) {
            return option.cast(Integer.valueOf(getHighWater(updateWaterMark(-1, Options.CONNECTION_HIGH_WATER.cast(value, Integer.valueOf((int) (CONN_HIGH_MASK >> CONN_HIGH_BIT))).intValue()))));
        } else if (option == Options.CONNECTION_LOW_WATER) {
            return option.cast(Integer.valueOf(getLowWater(updateWaterMark(Options.CONNECTION_LOW_WATER.cast(value, Integer.valueOf((int) (CONN_LOW_MASK >> CONN_LOW_BIT))).intValue(), -1))));
        } else {
            return super.setOption(option, value);
        }
    }

}
