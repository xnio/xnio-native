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

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.xnio.ChannelListener;
import org.xnio.ChannelListeners;
import org.xnio.IoUtils;
import org.xnio.Option;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.XnioExecutor;
import org.xnio.XnioIoThread;
import org.xnio.channels.AcceptListenerSettable;
import org.xnio.channels.CloseListenerSettable;
import org.xnio.channels.SuspendableAcceptChannel;
import org.xnio.channels.UnsupportedOptionException;
import org.xnio.management.XnioServerMXBean;

import static org.xnio.IoUtils.safeClose;
import static org.xnio.nativeimpl.Log.log;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
abstract class NativeAcceptChannel<C extends NativeAcceptChannel<C>> implements SuspendableAcceptChannel, AcceptListenerSettable<C>, CloseListenerSettable<C> {

    private volatile ChannelListener<? super C> acceptListener;
    private volatile ChannelListener<? super C> closeListener;

    private final SocketAddress localAddress;
    private final AcceptChannelHandle[] handles;
    private final int fd;
    private final NativeXnioWorker worker;
    private final Closeable mbeanHandle;
    private final AtomicBoolean closed = new AtomicBoolean();

    private volatile long connectionStatus = CONN_LOW_MASK | CONN_HIGH_MASK;
    @SuppressWarnings("unused")
    private volatile int readTimeout;
    @SuppressWarnings("unused")
    private volatile int writeTimeout;
    private volatile int tokenConnectionCount;
    volatile boolean resumed;

    private static final long CONN_LOW_MASK     = 0x000000007FFFFFFFL;
    private static final long CONN_LOW_BIT      = 0L;
    @SuppressWarnings("unused")
    private static final long CONN_LOW_ONE      = 1L;
    private static final long CONN_HIGH_MASK    = 0x3FFFFFFF80000000L;
    private static final long CONN_HIGH_BIT     = 31L;
    @SuppressWarnings("unused")
    private static final long CONN_HIGH_ONE     = 1L << CONN_HIGH_BIT;

    private static final AtomicIntegerFieldUpdater<NativeAcceptChannel> readTimeoutUpdater = AtomicIntegerFieldUpdater.newUpdater(NativeAcceptChannel.class, "readTimeout");
    private static final AtomicIntegerFieldUpdater<NativeAcceptChannel> writeTimeoutUpdater = AtomicIntegerFieldUpdater.newUpdater(NativeAcceptChannel.class, "writeTimeout");

    @SuppressWarnings("rawtypes")
    private static final AtomicLongFieldUpdater<NativeAcceptChannel> connectionStatusUpdater = AtomicLongFieldUpdater.newUpdater(NativeAcceptChannel.class, "connectionStatus");

    NativeAcceptChannel(final NativeXnioWorker worker, final int fd, final OptionMap optionMap) throws IOException {
        this.worker = worker;
        this.fd = fd;
        localAddress = Native.getSocketAddress(Native.getSockName(fd));
        final NativeWorkerThread[] threads = worker.getAll();
        final int threadCount = threads.length;
        if (threadCount == 0) {
            throw log.noThreads();
        }
        final int tokens = optionMap.get(Options.BALANCING_TOKENS, -1);
        final int connections = optionMap.get(Options.BALANCING_CONNECTIONS, 16);
        if (tokens != -1) {
            if (tokens < 1 || tokens >= threadCount) {
                throw log.balancingTokens();
            }
            if (connections < 1) {
                throw log.balancingConnectionCount();
            }
            tokenConnectionCount = connections;
        }
        if (optionMap.contains(Options.READ_TIMEOUT)) {
            readTimeoutUpdater.lazySet(this, optionMap.get(Options.READ_TIMEOUT, 0));
        }
        if (optionMap.contains(Options.WRITE_TIMEOUT)) {
            writeTimeoutUpdater.lazySet(this, optionMap.get(Options.WRITE_TIMEOUT, 0));
        }
        int perThreadLow, perThreadLowRem;
        int perThreadHigh, perThreadHighRem;
        if (optionMap.contains(Options.CONNECTION_HIGH_WATER) || optionMap.contains(Options.CONNECTION_LOW_WATER)) {
            final int highWater = optionMap.get(Options.CONNECTION_HIGH_WATER, Integer.MAX_VALUE);
            final int lowWater = optionMap.get(Options.CONNECTION_LOW_WATER, highWater);
            if (highWater <= 0) {
                throw badHighWater();
            }
            if (lowWater <= 0 || lowWater > highWater) {
                throw badLowWater(highWater);
            }
            final long highLowWater = (long) highWater << CONN_HIGH_BIT | (long) lowWater << CONN_LOW_BIT;
            connectionStatusUpdater.lazySet(this, highLowWater);
            perThreadLow = lowWater / threadCount;
            perThreadLowRem = lowWater % threadCount;
            perThreadHigh = highWater / threadCount;
            perThreadHighRem = highWater % threadCount;
        } else {
            perThreadLow = Integer.MAX_VALUE;
            perThreadLowRem = 0;
            perThreadHigh = Integer.MAX_VALUE;
            perThreadHighRem = 0;
            connectionStatusUpdater.lazySet(this, CONN_LOW_MASK | CONN_HIGH_MASK);
        }
        final AcceptChannelHandle[] handles = new AcceptChannelHandle[threadCount];
        for (int i = 0; i < threadCount; i++) {
            AcceptChannelHandle handle = new AcceptChannelHandle(this, fd, threads[i], i < perThreadHighRem ? perThreadHigh + 1 : perThreadHigh, i < perThreadLowRem ? perThreadLow + 1 : perThreadLow);
            handles[i] = handle;
        }
        this.handles = handles;
        if (tokens > 0) {
            for (int i = 0; i < threadCount; i ++) {
                handles[i].initializeTokenCount(i < tokens ? connections : 0);
            }
        }
        mbeanHandle = NativeXnio.register(new XnioServerMXBean() {
            public String getProviderName() {
                return "native";
            }

            public String getWorkerName() {
                return getWorker().getName();
            }

            public String getBindAddress() {
                return String.valueOf(getLocalAddress());
            }

            public int getConnectionCount() {
                final AtomicInteger counter = new AtomicInteger();
                final CountDownLatch latch = new CountDownLatch(handles.length);
                for (final AcceptChannelHandle handle : handles) try {
                    handle.thread.execute(new Runnable() {
                        public void run() {
                            counter.getAndAdd(handle.getConnectionCount());
                            latch.countDown();
                        }
                    });
                } catch (Throwable ignored) {
                    latch.countDown();
                }
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return counter.get();
            }

            public int getConnectionLimitHighWater() {
                return getHighWater(connectionStatus);
            }

            public int getConnectionLimitLowWater() {
                return getLowWater(connectionStatus);
            }
        });
    }

    void register() throws IOException {
        for (AcceptChannelHandle handle : handles) {
            handle.thread.register(handle);
        }
    }

    private static IllegalArgumentException badLowWater(final int highWater) {
        return new IllegalArgumentException("Low water must be greater than 0 and less than or equal to high water (" + highWater + ")");
    }

    private static IllegalArgumentException badHighWater() {
        return new IllegalArgumentException("High water must be greater than 0");
    }

    public void close() throws IOException {
        if (! closed.getAndSet(true)) {
            for (AcceptChannelHandle handle : handles) {
                handle.unregister();
            }
            safeClose(mbeanHandle);
            Native.testAndThrow(Native.dup2(Native.DEAD_FD, fd));
            new FdRef<NativeAcceptChannel>(this, fd);
        }
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
            throw new UnsupportedOptionException();
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
            throw new UnsupportedOptionException();
        }
    }

    private long updateWaterMark(int reqNewLowWater, int reqNewHighWater) {
        // at least one must be specified
        assert reqNewLowWater != -1 || reqNewHighWater != -1;
        // if both given, low must be less than high
        assert reqNewLowWater == -1 || reqNewHighWater == -1 || reqNewLowWater <= reqNewHighWater;

        long oldVal, newVal;
        int oldHighWater, oldLowWater;
        int newLowWater, newHighWater;

        do {
            oldVal = connectionStatus;
            oldLowWater = getLowWater(oldVal);
            oldHighWater = getHighWater(oldVal);
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
            newVal = (long)newLowWater << CONN_LOW_BIT | (long)newHighWater << CONN_HIGH_BIT;
        } while (! connectionStatusUpdater.compareAndSet(this, oldVal, newVal));

        final AcceptChannelHandle[] conduits = handles;
        final int threadCount = conduits.length;

        int perThreadLow, perThreadLowRem;
        int perThreadHigh, perThreadHighRem;

        perThreadLow = newLowWater / threadCount;
        perThreadLowRem = newLowWater % threadCount;
        perThreadHigh = newHighWater / threadCount;
        perThreadHighRem = newHighWater % threadCount;

        for (int i = 0; i < conduits.length; i++) {
            AcceptChannelHandle conduit = conduits[i];
            conduit.executeSetTask(i < perThreadHighRem ? perThreadHigh + 1 : perThreadHigh, i < perThreadLowRem ? perThreadLow + 1 : perThreadLow);
        }

        return oldVal;
    }

    private static int getHighWater(final long value) {
        return (int) ((value & CONN_HIGH_MASK) >> CONN_HIGH_BIT);
    }

    private static int getLowWater(final long value) {
        return (int) ((value & CONN_LOW_MASK) >> CONN_LOW_BIT);
    }

    protected abstract NativeStreamConnection constructConnection(int fd, NativeWorkerThread thread, final AcceptChannelHandle acceptChannelHandle);

    public NativeStreamConnection accept() throws IOException {
        final NativeWorkerThread current = NativeWorkerThread.getCurrent();
        final AcceptChannelHandle handle = handles[current.getNumber()];
        if (! handle.getConnection()) {
            log.tracef("Connections full on %s", this);
            return null;
        }
        final int accepted = Native.accept(fd);
        boolean ok = false;
        try {
            if (accepted == -Native.EAGAIN) {
                if (Native.EXTRA_TRACE) log.tracef("Accept would block on %s", this);
                return null;
            }
            Native.testAndThrow(accepted);

            try {
                final NativeStreamConnection newConnection = constructConnection(accepted, current, handle);
                newConnection.setOption(Options.READ_TIMEOUT, Integer.valueOf(readTimeout));
                newConnection.setOption(Options.WRITE_TIMEOUT, Integer.valueOf(writeTimeout));
                current.register(newConnection.conduit);
                if (Native.EXTRA_TRACE) log.tracef("Accept(%d): %d", fd, accepted);
                ok = true;
                return newConnection;
            } finally {
                if (! ok) Native.close(accepted);
            }
        } finally {
            if (! ok) {
                handle.freeConnection();
            }
        }
    }

    public String toString() {
        return String.format("%s fd=%d", getClass().getName(), Integer.valueOf(fd));
    }

    public ChannelListener<? super C> getAcceptListener() {
        return acceptListener;
    }

    public void setAcceptListener(final ChannelListener<? super C> acceptListener) {
        this.acceptListener = acceptListener;
    }

    public ChannelListener.Setter<C> getAcceptSetter() {
        return new AcceptListenerSettable.Setter<C>(this);
    }

    public boolean isOpen() {
        return ! closed.get();
    }

    public SocketAddress getLocalAddress() {
        return localAddress;
    }

    public <A extends SocketAddress> A getLocalAddress(final Class<A> type) {
        final SocketAddress address = getLocalAddress();
        return type.isInstance(address) ? type.cast(address) : null;
    }

    public void suspendAccepts() {
        resumed = false;
        for (AcceptChannelHandle handle : handles) {
            handle.suspend();
        }
    }

    public void resumeAccepts() {
        resumed = true;
        for (AcceptChannelHandle handle : handles) {
            handle.resume();
        }
    }

    public void wakeupAccepts() {
        resumeAccepts();
        final AcceptChannelHandle[] handles = this.handles;
        final int idx = IoUtils.getThreadLocalRandom().nextInt(handles.length);
        handles[idx].thread.execute(new Runnable() {
            public void run() {
                invokeAcceptHandler();
            }
        });
    }

    public void awaitAcceptable() throws IOException {
        throw log.unsupported("awaitAcceptable");
    }

    public void awaitAcceptable(final long time, final TimeUnit timeUnit) throws IOException {
        throw log.unsupported("awaitAcceptable");
    }

    public boolean isAcceptResumed() {
        return resumed;
    }

    @Deprecated
    public XnioExecutor getAcceptThread() {
        return getIoThread();
    }

    AcceptChannelHandle getHandle(final int number) {
        return handles[number];
    }

    int getTokenConnectionCount() {
        return tokenConnectionCount;
    }

    @SuppressWarnings("unchecked")
    void invokeAcceptHandler() {
        ChannelListeners.invokeChannelListener((C) this, acceptListener);
    }

    public ChannelListener.Setter<C> getCloseSetter() {
        return new CloseListenerSettable.Setter<C>(this);
    }

    public ChannelListener<? super C> getCloseListener() {
        return closeListener;
    }

    public void setCloseListener(final ChannelListener<? super C> listener) {
        this.closeListener = listener;
    }

    public NativeXnioWorker getWorker() {
        return worker;
    }

    public XnioIoThread getIoThread() {
        return getWorker().chooseThread();
    }
}
