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
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import org.xnio.Bits;
import org.xnio.ChannelListener;
import org.xnio.ChannelListeners;
import org.xnio.ClosedWorkerException;
import org.xnio.IoUtils;
import org.xnio.LocalSocketAddress;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.StreamConnection;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.channels.MulticastMessageChannel;

import static java.util.concurrent.locks.LockSupport.unpark;
import static org.xnio.nativeimpl.Log.log;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class NativeXnioWorker extends XnioWorker {

    private static final int CLOSE_REQ = (1 << 31);
    private static final int CLOSE_COMP = (1 << 30);

    // start at 1 for the provided thread pool
    private volatile int state = 1;

    private final NativeWorkerThread[] workerThreads;

    @SuppressWarnings("unused")
    private volatile Thread shutdownWaiter;

    private static final AtomicReferenceFieldUpdater<NativeXnioWorker, Thread> shutdownWaiterUpdater = AtomicReferenceFieldUpdater.newUpdater(NativeXnioWorker.class, Thread.class, "shutdownWaiter");

    private static final AtomicIntegerFieldUpdater<NativeXnioWorker> stateUpdater = AtomicIntegerFieldUpdater.newUpdater(NativeXnioWorker.class, "state");

    @SuppressWarnings("deprecation")
    NativeXnioWorker(final NativeXnio xnio, final ThreadGroup threadGroup, final OptionMap optionMap, final Runnable terminationTask) throws IOException {
        super(xnio, threadGroup, optionMap, terminationTask);
        final int threadCount;
        if (optionMap.contains(Options.WORKER_IO_THREADS)) {
            threadCount = optionMap.get(Options.WORKER_IO_THREADS, 0);
        } else {
            threadCount = Math.max(optionMap.get(Options.WORKER_READ_THREADS, 1), optionMap.get(Options.WORKER_WRITE_THREADS, 1));
        }
        if (threadCount < 0) {
            throw log.optionOutOfRange("WORKER_IO_THREADS");
        }
        final long workerStackSize = optionMap.get(Options.STACK_SIZE, 0L);
        if (workerStackSize < 0L) {
            throw log.optionOutOfRange("STACK_SIZE");

        }
        String workerName = getName();
        NativeWorkerThread[] workerThreads;
        workerThreads = new NativeWorkerThread[threadCount];
        final boolean markWorkerThreadAsDaemon = optionMap.get(Options.THREAD_DAEMON, false);
        boolean ok = false;
        try {
            for (int i = 0; i < threadCount; i++) {
                final NativeWorkerThread readWorker;
                if (Native.HAS_EPOLL) {
                    readWorker = new EPollWorkerThread(this, i, String.format("%s I/O-%d", workerName, Integer.valueOf(i + 1)), threadGroup, workerStackSize);
                } else {
                    throw new IOException("No suitable worker implementations available");
                }
                // Mark as daemon if the Options.THREAD_DAEMON has been set
                if (markWorkerThreadAsDaemon) {
                    readWorker.setDaemon(true);
                }
                workerThreads[i] = readWorker;
            }
            ok = true;
        } finally {
            if (! ok) {
                for (NativeWorkerThread worker : workerThreads) {
                    if (worker != null) {
                        worker.close();
                    }
                }
            }
        }
        this.workerThreads = workerThreads;
    }

    void start() {
        for (NativeWorkerThread worker : workerThreads) {
            openResourceUnconditionally();
            worker.start();
        }
    }

    protected NativeWorkerThread chooseThread() {
        final NativeWorkerThread[] workers = this.workerThreads;
        final int length = workers.length;
        if (length == 0) {
            throw log.noThreads();
        }
        if (length == 1) {
            return workers[0];
        }
        final Random random = IoUtils.getThreadLocalRandom();
        return workers[random.nextInt(length)];
    }

    public int getIoThreadCount() {
        return workerThreads.length;
    }

    NativeWorkerThread[] getAll() {
        return workerThreads;
    }

    protected AcceptingChannel<StreamConnection> createTcpConnectionServer(final InetSocketAddress bindAddress, final ChannelListener<? super AcceptingChannel<StreamConnection>> acceptListener, final OptionMap optionMap) throws IOException {
        checkShutdown();
        final InetAddress address = bindAddress.getAddress();
        final int fd;
        if (address instanceof Inet4Address) {
            fd = Native.socketTcp();
        } else if (address instanceof Inet6Address) {
            fd = Native.socketTcp6();
        } else {
            throw new IllegalArgumentException("Unknown address format");
        }
        Native.testAndThrow(fd);
        Native.testAndThrow(Native.setOptReuseAddr(fd, optionMap.get(Options.REUSE_ADDRESSES, true)));
        boolean ok = false;
        try {
            Native.testAndThrow(Native.bind(fd, Native.encodeSocketAddress(bindAddress)));
            final TcpServer server = new TcpServer(this, fd, optionMap);
            Native.testAndThrow(Native.listen(fd, optionMap.get(Options.BACKLOG, 128)));
            server.setAcceptListener(acceptListener);
            server.register();
            ok = true;
            return server;
        } finally {
            if (! ok) {
                Native.close(fd);
            }
        }
    }

    protected AcceptingChannel<StreamConnection> createLocalStreamConnectionServer(final LocalSocketAddress bindAddress, final ChannelListener<? super AcceptingChannel<StreamConnection>> acceptListener, final OptionMap optionMap) throws IOException {
        checkShutdown();
        final int fd = Native.socketLocalStream();
        Native.testAndThrow(fd);
        boolean ok = false;
        try {
            Native.testAndThrow(Native.bind(fd, Native.encodeSocketAddress(bindAddress)));
            final UnixServer server = new UnixServer(this, fd, optionMap);
            Native.testAndThrow(Native.listen(fd, optionMap.get(Options.BACKLOG, 128)));
            server.setAcceptListener(acceptListener);
            server.register();
            ok = true;
            return server;
        } finally {
            if (! ok) {
                Native.close(fd);
            }
        }
    }

    /** {@inheritDoc} */
    public MulticastMessageChannel createUdpServer(final InetSocketAddress bindAddress, final ChannelListener<? super MulticastMessageChannel> bindListener, final OptionMap optionMap) throws IOException {
        if (true) throw new IOException("Not implemented yet");
        checkShutdown();
        final InetAddress address = bindAddress.getAddress();
        final int fd;
        if (address instanceof Inet4Address) {
            fd = Native.testAndThrow(Native.socketUdp());
        } else if (address instanceof Inet6Address) {
            fd = Native.testAndThrow(Native.socketUdp6());
        } else {
            throw new IllegalArgumentException("Unknown address format");
        }
        boolean ok = false;
        try {
//            if (optionMap.contains(Options.BROADCAST))
//            if (optionMap.contains(Options.IP_TRAFFIC_CLASS))
//            if (optionMap.contains(Options.RECEIVE_BUFFER))
//            channel.socket().setReuseAddress(optionMap.get(Options.REUSE_ADDRESSES, true));
//            if (optionMap.contains(Options.SEND_BUFFER))
            Native.testAndThrow(Native.bind(fd, Native.encodeSocketAddress(bindAddress)));
            ChannelListeners.invokeChannelListener(null, bindListener);
            return null;
        } finally {
            if (! ok) {
                Native.close(fd);
            }
        }
    }

    public boolean isShutdown() {
        return (state & CLOSE_REQ) != 0;
    }

    public boolean isTerminated() {
        return (state & CLOSE_COMP) != 0;
    }

    /**
     * Open a resource unconditionally (i.e. accepting a connection on an open server).
     */
    void openResourceUnconditionally() {
        int oldState = stateUpdater.getAndIncrement(this);
        if (log.isTraceEnabled()) {
            log.tracef("CAS %s %08x -> %08x", this, Integer.valueOf(oldState), Integer.valueOf(oldState + 1));
        }
    }

    void checkShutdown() throws ClosedWorkerException {
        if (isShutdown())
            throw log.workerShutDown();
    }

    void closeResource() {
        int oldState = stateUpdater.decrementAndGet(this);
        if (log.isTraceEnabled()) {
            log.tracef("CAS %s %08x -> %08x", this, Integer.valueOf(oldState + 1), Integer.valueOf(oldState));
        }
        while (oldState == CLOSE_REQ) {
            if (stateUpdater.compareAndSet(this, CLOSE_REQ, CLOSE_REQ | CLOSE_COMP)) {
                log.tracef("CAS %s %08x -> %08x (close complete)", this, Integer.valueOf(CLOSE_REQ), Integer.valueOf(CLOSE_REQ | CLOSE_COMP));
                safeUnpark(shutdownWaiterUpdater.getAndSet(this, null));
                final Runnable task = getTerminationTask();
                if (task != null) try {
                    task.run();
                } catch (Throwable ignored) {}
                return;
            }
            oldState = state;
        }
    }

    public void shutdown() {
        int oldState;
        oldState = state;
        while ((oldState & CLOSE_REQ) == 0) {
            // need to do the close ourselves...
            if (! stateUpdater.compareAndSet(this, oldState, oldState | CLOSE_REQ)) {
                // changed in the meantime
                oldState = state;
                continue;
            }
            log.tracef("Initiating shutdown of %s", this);
            for (NativeWorkerThread worker : workerThreads) {
                worker.shutdown();
            }
            shutDownTaskPool();
            return;
        }
        log.tracef("Idempotent shutdown of %s", this);
    }

    public List<Runnable> shutdownNow() {
        shutdown();
        return shutDownTaskPoolNow();
    }

    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        int oldState = state;
        if (Bits.allAreSet(oldState, CLOSE_COMP)) {
            return true;
        }
        long then = System.nanoTime();
        long duration = unit.toNanos(timeout);
        final Thread myThread = Thread.currentThread();
        while (Bits.allAreClear(oldState = state, CLOSE_COMP)) {
            final Thread oldThread = shutdownWaiterUpdater.getAndSet(this, myThread);
            try {
                if (Bits.allAreSet(oldState = state, CLOSE_COMP)) {
                    break;
                }
                LockSupport.parkNanos(this, duration);
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                long now = System.nanoTime();
                duration -= now - then;
                if (duration < 0L) {
                    oldState = state;
                    break;
                }
            } finally {
                safeUnpark(oldThread);
            }
        }
        return Bits.allAreSet(oldState, CLOSE_COMP);
    }

    public void awaitTermination() throws InterruptedException {
        int oldState = state;
        if (Bits.allAreSet(oldState, CLOSE_COMP)) {
            return;
        }
        final Thread myThread = Thread.currentThread();
        while (Bits.allAreClear(state, CLOSE_COMP)) {
            final Thread oldThread = shutdownWaiterUpdater.getAndSet(this, myThread);
            try {
                if (Bits.allAreSet(state, CLOSE_COMP)) {
                    break;
                }
                LockSupport.park(this);
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
            } finally {
                safeUnpark(oldThread);
            }
        }
    }

    private static void safeUnpark(final Thread waiter) {
        if (waiter != null) unpark(waiter);
    }

    protected void taskPoolTerminated() {
        closeResource();
    }

    public NativeXnio getXnio() {
        return (NativeXnio) super.getXnio();
    }
}
