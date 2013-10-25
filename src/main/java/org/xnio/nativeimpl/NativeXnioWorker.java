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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import org.jboss.logging.Logger;
import org.xnio.Bits;
import org.xnio.Cancellable;
import org.xnio.ChannelListener;
import org.xnio.ChannelListeners;
import org.xnio.ChannelPipe;
import org.xnio.ClosedWorkerException;
import org.xnio.FailedIoFuture;
import org.xnio.FinishedIoFuture;
import org.xnio.FutureResult;
import org.xnio.IoFuture;
import org.xnio.IoUtils;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.channels.BoundChannel;
import org.xnio.channels.CloseableChannel;
import org.xnio.channels.ConnectedStreamChannel;
import org.xnio.channels.StreamChannel;
import org.xnio.channels.StreamSinkChannel;
import org.xnio.channels.StreamSourceChannel;

import static java.util.concurrent.locks.LockSupport.unpark;
import static org.xnio.IoUtils.safeClose;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class NativeXnioWorker extends XnioWorker {

    private static final Logger log = Logger.getLogger("org.xnio.native");

    private static final int CLOSE_REQ = (1 << 31);
    private static final int CLOSE_COMP = (1 << 30);

    // start at 1 for the provided thread pool
    private volatile int state = 1;

    private final NativeWorkerThread[] readWorkers;
    private final NativeWorkerThread[] writeWorkers;

    @SuppressWarnings("unused")
    private volatile Thread shutdownWaiter;

    private static final AtomicReferenceFieldUpdater<NativeXnioWorker, Thread> shutdownWaiterUpdater = AtomicReferenceFieldUpdater.newUpdater(NativeXnioWorker.class, Thread.class, "shutdownWaiter");

    private static final AtomicIntegerFieldUpdater<NativeXnioWorker> stateUpdater = AtomicIntegerFieldUpdater.newUpdater(NativeXnioWorker.class, "state");

    NativeXnioWorker(final NativeXnio xnio, final ThreadGroup threadGroup, final OptionMap optionMap, final Runnable terminationTask) throws IOException {
        super(xnio, threadGroup, optionMap, terminationTask);
        final int readCount = optionMap.get(Options.WORKER_READ_THREADS, 1);
        if (readCount < 0) {
            throw new IllegalArgumentException("Worker read thread count must be >= 0");
        }
        final int writeCount = optionMap.get(Options.WORKER_WRITE_THREADS, 1);
        if (writeCount < 0) {
            throw new IllegalArgumentException("Worker write thread count must be >= 0");
        }
        final long workerStackSize = optionMap.get(Options.STACK_SIZE, 0L);
        if (workerStackSize < 0L) {
            throw new IllegalArgumentException("Worker stack size must be >= 0");
        }
        String workerName = getName();
        NativeWorkerThread[] readWorkers, writeWorkers;
        readWorkers = new NativeWorkerThread[readCount];
        writeWorkers = new NativeWorkerThread[writeCount];
        final boolean markWorkerThreadAsDaemon = optionMap.get(Options.THREAD_DAEMON, false);
        boolean ok = false;
        try {
            for (int i = 0; i < readCount; i++) {
                final NativeWorkerThread readWorker;
                if (Native.HAS_EPOLL) {
                    readWorker = new EPollWorkerThread(this, String.format("%s read-%d", workerName, Integer.valueOf(i + 1)), threadGroup, workerStackSize, false);
                } else {
                    throw new IOException("No suitable worker implementations available");
                }
                // Mark as daemon if the Options.THREAD_DAEMON has been set
                if (markWorkerThreadAsDaemon) {
                    readWorker.setDaemon(true);
                }
                readWorkers[i] = readWorker;
                readWorker.start();
            }
            for (int i = 0; i < writeCount; i++) {
                final NativeWorkerThread writeWorker;
                if (Native.HAS_EPOLL) {
                    writeWorker = new EPollWorkerThread(this, String.format("%s write-%d", workerName, Integer.valueOf(i + 1)), threadGroup, workerStackSize, true);
                } else {
                    throw new IOException("No suitable worker implementations available");
                }
                // Mark as daemon if the Options.THREAD_DAEMON has been set
                if (markWorkerThreadAsDaemon) {
                    writeWorker.setDaemon(true);
                }
                writeWorkers[i] = writeWorker;
                writeWorker.start();
            }
            ok = true;
        } finally {
            if (! ok) {
                for (NativeWorkerThread worker : readWorkers) {
                    if (worker != null) {
                        worker.exit();
                    }
                }
                for (NativeWorkerThread worker : writeWorkers) {
                    if (worker != null) {
                        worker.exit();
                    }
                }
            }
        }
        this.readWorkers = readWorkers;
        this.writeWorkers = writeWorkers;
    }

    void start() {
        for (NativeWorkerThread worker : readWorkers) {
            openResourceUnconditionally();
            worker.start();
        }
        for (NativeWorkerThread worker : writeWorkers) {
            openResourceUnconditionally();
            worker.start();
        }
    }

    private static final NativeWorkerThread[] NO_WORKERS = new NativeWorkerThread[0];

    NativeWorkerThread choose() {
        final NativeWorkerThread[] write = writeWorkers;
        final NativeWorkerThread[] read = readWorkers;
        final int writeLength = write.length;
        final int readLength = read.length;
        if (writeLength == 0) {
            return choose(false);
        }
        if (readLength == 0) {
            return choose(true);
        }
        final Random random = IoUtils.getThreadLocalRandom();
        final int idx = random.nextInt(writeLength + readLength);
        return idx >= readLength ? write[idx - readLength] : read[idx];
    }

    NativeWorkerThread chooseOptional(final boolean write) {
        final NativeWorkerThread[] orig = write ? writeWorkers : readWorkers;
        final int length = orig.length;
        if (length == 0) {
            return null;
        }
        if (length == 1) {
            return orig[0];
        }
        final Random random = IoUtils.getThreadLocalRandom();
        return orig[random.nextInt(length)];
    }

    NativeWorkerThread choose(final boolean write) {
        final NativeWorkerThread result = chooseOptional(write);
        if (result == null) {
            throw new IllegalArgumentException("No threads configured");
        }
        return result;
    }

    NativeWorkerThread[] choose(int count, boolean write) {
        if (count == 0) {
            return NO_WORKERS;
        }
        final NativeWorkerThread[] orig = write ? writeWorkers : readWorkers;
        final int length = orig.length;
        final int halfLength = length >> 1;
        if (length == 0) {
            throw new IllegalArgumentException("No threads configured");
        }
        if (count == length) {
            return orig;
        }
        if (count > length) {
            throw new IllegalArgumentException("Not enough " + (write ? "write" : "read") + " threads configured");
        }
        final NativeWorkerThread[] result = new NativeWorkerThread[count];
        final Random random = IoUtils.getThreadLocalRandom();
        if (count == 1) {
            result[0] = orig[random.nextInt(length)];
            return result;
        }
        if (length < 32) {
            if (count >= halfLength) {
                int bits = (1 << length) - 1;
                do {
                    bits &= ~(1 << random.nextInt(length));
                } while (Integer.bitCount(bits) > count);
                for (int i = 0; i < count; i ++) {
                    final int bit = Integer.numberOfTrailingZeros(bits);
                    result[i] = orig[bit];
                    bits ^= Integer.lowestOneBit(bits);
                }
                return result;
            } else {
                int bits = 0;
                do {
                    bits |= (1 << random.nextInt(length));
                } while (Integer.bitCount(bits) < count);
                for (int i = 0; i < count; i ++) {
                    final int bit = Integer.numberOfTrailingZeros(bits);
                    result[i] = orig[bit];
                    bits ^= Integer.lowestOneBit(bits);
                }
                return result;
            }
        }
        if (length < 64) {
            if (count >= halfLength) {
                long bits = (1L << (long) length) - 1L;
                do {
                    bits &= ~(1L << (long) random.nextInt(length));
                } while (Long.bitCount(bits) > count);
                for (int i = 0; i < count; i ++) {
                    final int bit = Long.numberOfTrailingZeros(bits);
                    result[i] = orig[bit];
                    bits ^= Long.lowestOneBit(bits);
                }
                return result;
            } else {
                long bits = 0;
                do {
                    bits |= (1L << (long) random.nextInt(length));
                } while (Long.bitCount(bits) < count);
                for (int i = 0; i < count; i ++) {
                    final int bit = Long.numberOfTrailingZeros(bits);
                    result[i] = orig[bit];
                    bits ^= Long.lowestOneBit(bits);
                }
                return result;
            }
        }
        // lots of threads.  No faster way to do it.
        final HashSet<NativeWorkerThread> set;
        if (count >= halfLength) {
            // We're returning half or more of the threads.
            set = new HashSet<NativeWorkerThread>(Arrays.asList(orig));
            while (set.size() > count) {
                set.remove(orig[random.nextInt(length)]);
            }
        } else {
            // We're returning less than half of the threads.
            set = new HashSet<NativeWorkerThread>(length);
            while (set.size() < count) {
                set.add(orig[random.nextInt(length)]);
            }
        }
        return set.toArray(result);
    }

    protected AcceptingChannel<? extends ConnectedStreamChannel> createTcpServer(final InetSocketAddress bindAddress, final ChannelListener<? super AcceptingChannel<ConnectedStreamChannel>> acceptListener, final OptionMap optionMap) throws IOException {
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
        boolean ok = false;
        try {
            Native.testAndThrow(Native.bind(fd, Native.encodeSocketAddress(bindAddress)));
            final TcpServer server = new TcpServer(this, fd, optionMap);
            // cast is not really needed
            server.setAcceptListener((ChannelListener<? super TcpServer>) acceptListener);
            Native.testAndThrow(Native.listen(fd, optionMap.get(Options.BACKLOG, 128)));
            server.resumeAccepts();
            ok = true;
            return server;
        } finally {
            if (! ok) {
                Native.close(fd);
            }
        }
    }

    protected IoFuture<ConnectedStreamChannel> connectTcpStream(final InetSocketAddress bindAddress, final InetSocketAddress destinationAddress, final ChannelListener<? super ConnectedStreamChannel> openListener, final ChannelListener<? super BoundChannel> bindListener, final OptionMap optionMap) {
        try {
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
            boolean ok = false;
            try {
                Native.testAndThrow(Native.bind(fd, Native.encodeSocketAddress(bindAddress)));
                final NativeWorkerThread readThread, writeThread;
                writeThread = choose(true);
                readThread = choose(false);
                int res = Native.connect(fd, Native.encodeSocketAddress(destinationAddress));
                final FutureResult<ConnectedStreamChannel> futureResult = new FutureResult<ConnectedStreamChannel>();
                final NativeWorkerThread connectThread = optionMap.get(Options.WORKER_ESTABLISH_WRITING, false) ? writeThread : readThread;
                if (res == -Native.EAGAIN) {
                    // blocking
                    final TcpConnectChannel connectChannel = new TcpConnectChannel(this, fd, futureResult, optionMap, openListener, readThread, writeThread);
                    log.tracef("Blocking connect of %s", connectChannel);
                    connectThread.register(connectChannel);
                    futureResult.addCancelHandler(new Cancellable() {
                        public Cancellable cancel() {
                            if (futureResult.setCancelled()) {
                                connectChannel.suppressFinalize();
                                Native.close(fd);
                            }
                            return this;
                        }
                    });
                    connectThread.resumeFd(fd, false, true);
                    ok = true;
                    return futureResult.getIoFuture();
                } else {
                    Native.testAndThrow(res);
                    final TcpSocketChannel channel = new TcpSocketChannel(this, fd, readThread, writeThread, null);
                    readThread.register(channel);
                    writeThread.register(channel);
                    log.tracef("Non-blocking connect of %s", channel);
                    // not unsafe - http://youtrack.jetbrains.net/issue/IDEA-59290
                    //noinspection unchecked
                    connectThread.execute(ChannelListeners.getChannelListenerTask(channel, openListener));
                    ok = true;
                    return new FinishedIoFuture<ConnectedStreamChannel>(channel);
                }
            } finally {
                if (! ok) Native.close(fd);
            }
        } catch (IOException e) {
            return new FailedIoFuture<ConnectedStreamChannel>(e);
        }
    }

    protected IoFuture<ConnectedStreamChannel> acceptTcpStream(final InetSocketAddress destination, final ChannelListener<? super ConnectedStreamChannel> openListener, final ChannelListener<? super BoundChannel> bindListener, final OptionMap optionMap) {
        try {
            final InetAddress address = destination.getAddress();
            final int fd;
            if (address instanceof Inet4Address) {
                fd = Native.socketTcp();
            } else if (address instanceof Inet6Address) {
                fd = Native.socketTcp6();
            } else {
                throw new IllegalArgumentException("Unknown address format");
            }
            Native.testAndThrow(fd);
            boolean ok = false;
            try {
                if (optionMap.get(Options.REUSE_ADDRESSES, true)) {
                    Native.testAndThrow(Native.setOptReuseAddr(fd, true));
                }
                Native.testAndThrow(Native.bind(fd, Native.encodeSocketAddress(destination)));
                Native.testAndThrow(Native.listen(fd, 1));
                final NativeWorkerThread readThread, writeThread;
                writeThread = choose(true);
                readThread = choose(false);
                final TcpSocketChannel channel = new TcpSocketChannel(this, fd, readThread, writeThread, null);
                readThread.register(channel);
                writeThread.register(channel);
                int res = Native.accept(fd);
                final FutureResult<ConnectedStreamChannel> futureResult = new FutureResult<ConnectedStreamChannel>();
                if (res == -Native.EAGAIN) {
                    log.tracef("Blocking accept of %s", channel);
                    // blocking
                    channel.getReadSetter().set(new ChannelListener<TcpSocketChannel>() {
                        public void handleEvent(final TcpSocketChannel channel) {
                            log.tracef("Resume blocking accept of %s", channel);
                            int res = Native.accept(fd);
                            if (res == -Native.EAGAIN) {
                                log.tracef("Accept of %s would block", channel);
                                return;
                            }
                            channel.suspendReads();
                            if (res < 0) {
                                safeClose(channel);
                                futureResult.setException(Native.exceptionFor(res));
                            }
                            channel.getReadSetter().set(null);
                            int res2 = Native.dup2(res, fd);
                            Native.close(res);
                            if (res2 < 0) {
                                safeClose(channel);
                                futureResult.setException(Native.exceptionFor(res2));
                            }
                            try {
                                // re-add since we dupped the old one out of existence
                                channel.getReadThread().doRegister(channel);
                                channel.getWriteThread().doRegister(channel);
                            } catch (IOException e) {
                                safeClose(channel);
                                futureResult.setException(e);
                            }
                            futureResult.setResult(channel);
                            if (! optionMap.get(Options.WORKER_ESTABLISH_WRITING, false)) {
                                ChannelListeners.<ConnectedStreamChannel>invokeChannelListener(channel, openListener);
                            } else {
                                // not unsafe - http://youtrack.jetbrains.net/issue/IDEA-59290
                                //noinspection unchecked
                                channel.getWriteThread().execute(ChannelListeners.getChannelListenerTask(channel, openListener));
                            }
                        }
                    });
                    futureResult.addCancelHandler(new Cancellable() {
                        public Cancellable cancel() {
                            if (futureResult.setCancelled()) {
                                safeClose(channel);
                            }
                            return this;
                        }

                        public String toString() {
                            return "Cancel handler for " + channel;
                        }
                    });
                    channel.resumeReads();
                    ok = true;
                    return futureResult.getIoFuture();
                } else {
                    log.tracef("Non-blocking accept of %s", channel);
                    int res2 = Native.dup2(res, fd);
                    Native.close(res);
                    if (res2 < 0) {
                        safeClose(channel);
                        futureResult.setException(Native.exceptionFor(res2));
                    }
                    try {
                        // re-add since we dupped the old one out of existence
                        channel.getReadThread().doRegister(channel);
                        channel.getWriteThread().doRegister(channel);
                    } catch (IOException e) {
                        safeClose(channel);
                        futureResult.setException(e);
                    }
                    final NativeWorkerThread workerThread = optionMap.get(Options.WORKER_ESTABLISH_WRITING, false) ? channel.getReadThread() : channel.getWriteThread();
                    // not unsafe - http://youtrack.jetbrains.net/issue/IDEA-59290
                    //noinspection unchecked
                    workerThread.execute(ChannelListeners.getChannelListenerTask(channel, openListener));
                    ok = true;
                    return new FinishedIoFuture<ConnectedStreamChannel>(channel);
                }
            } finally {
                if (! ok) Native.close(fd);
            }
        } catch (IOException e) {
            return new FailedIoFuture<ConnectedStreamChannel>(e);
        }
    }

    public ChannelPipe<StreamChannel, StreamChannel> createFullDuplexPipe() throws IOException {
        final int[] pair = new int[2];
        Native.testAndThrow(Native.socketPair(pair));
        final NativeWorkerThread leftReadThread = choose(false);
        final NativeWorkerThread leftWriteThread = choose(true);
        final UnixSocketChannel leftChannel = new UnixSocketChannel(this, pair[0], leftReadThread, leftWriteThread);
        leftReadThread.register(leftChannel);
        leftWriteThread.register(leftChannel);
        final NativeWorkerThread rightReadThread = choose(false);
        final NativeWorkerThread rightWriteThread = choose(true);
        final UnixSocketChannel rightChannel = new UnixSocketChannel(this, pair[1], rightReadThread, rightWriteThread);
        rightReadThread.register(rightChannel);
        rightWriteThread.register(rightChannel);
        return new ChannelPipe<StreamChannel, StreamChannel>(leftChannel, rightChannel);
    }

    public ChannelPipe<StreamSourceChannel, StreamSinkChannel> createHalfDuplexPipe() throws IOException {
        final int[] pair = new int[2];
        Native.testAndThrow(Native.pipe(pair));
        final NativeWorkerThread sourceReadThread = choose(false);
        final PipeSourceChannel source = new PipeSourceChannel(this, pair[0], sourceReadThread, null);
        sourceReadThread.register(source);
        final NativeWorkerThread sinkWriteThread = choose(true);
        final PipeSinkChannel sink = new PipeSinkChannel(this, pair[1], null, sinkWriteThread);
        sinkWriteThread.register(sink);
        return new ChannelPipe<StreamSourceChannel, StreamSinkChannel>(source, sink);
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

    /**
     * Open a resource.  Must be matched with a corresponding {@code closeResource()}.
     *
     * @throws org.xnio.ClosedWorkerException if the worker is closed
     */
    void openResource() throws ClosedWorkerException {
        int oldState;
        do {
            oldState = state;
            if ((oldState & CLOSE_REQ) != 0) {
                throw new ClosedWorkerException("Worker is shutting down");
            }
        } while (! stateUpdater.compareAndSet(this, oldState, oldState + 1));
        if (log.isTraceEnabled()) {
            log.tracef("CAS %s %08x -> %08x", this, Integer.valueOf(oldState), Integer.valueOf(oldState + 1));
        }
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

    private static void safeUnpark(final Thread thread) {
        if (thread != null) unpark(thread);
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
            for (NativeWorkerThread worker : readWorkers) {
                worker.shutdown();
            }
            for (NativeWorkerThread worker : writeWorkers) {
                worker.shutdown();
            }
            shutDownTaskPool();
            return;
        }
        log.tracef("Idempotent shutdown of %s", this);
        return;
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

    protected void doMigration(final CloseableChannel channel) throws IOException {
    }

    protected void taskPoolTerminated() {
        closeResource();
    }

    public NativeXnio getXnio() {
        return (NativeXnio) super.getXnio();
    }
}
