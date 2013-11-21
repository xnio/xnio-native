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
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.xnio.Cancellable;
import org.xnio.ChannelListener;
import org.xnio.ChannelListeners;
import org.xnio.ChannelPipe;
import org.xnio.ClosedWorkerException;
import org.xnio.FailedIoFuture;
import org.xnio.FinishedIoFuture;
import org.xnio.FutureResult;
import org.xnio.IoFuture;
import org.xnio.LocalSocketAddress;
import org.xnio.OptionMap;
import org.xnio.StreamConnection;
import org.xnio.XnioExecutor;
import org.xnio.XnioIoFactory;
import org.xnio.XnioIoThread;
import org.xnio.channels.BoundChannel;
import org.xnio.channels.StreamSinkChannel;
import org.xnio.channels.StreamSourceChannel;
import org.xnio.conduits.WriteReadyHandler;

import static org.xnio.nativeimpl.Log.log;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
abstract class NativeWorkerThread extends XnioIoThread implements XnioExecutor {

    private static final long LONGEST_DELAY = 9223372036853L;

    private volatile int state;

    private static final int SHUTDOWN = (1 << 31);

    private static final AtomicIntegerFieldUpdater<NativeWorkerThread> stateUpdater = AtomicIntegerFieldUpdater.newUpdater(NativeWorkerThread.class, "state");

    private final Object lock = new Object();

    private final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();

    NativeWorkerThread(final NativeXnioWorker worker, final int threadNumber, final String name, final ThreadGroup group, final long stackSize) {
        super(worker, threadNumber, group, name, stackSize);
    }

    static NativeWorkerThread getCurrent() {
        final Thread thread = currentThread();
        return thread instanceof NativeWorkerThread ? (NativeWorkerThread) thread : null;
    }

    public NativeXnioWorker getWorker() {
        return (NativeXnioWorker) super.getWorker();
    }

    protected IoFuture<StreamConnection> acceptTcpStreamConnection(final InetSocketAddress destination, final ChannelListener<? super StreamConnection> openListener, final ChannelListener<? super BoundChannel> bindListener, final OptionMap optionMap) {
        return acceptGeneralStreamConnection(destination, openListener, bindListener, optionMap);
    }

    protected IoFuture<StreamConnection> acceptLocalStreamConnection(final LocalSocketAddress destination, final ChannelListener<? super StreamConnection> openListener, final ChannelListener<? super BoundChannel> bindListener, final OptionMap optionMap) {
        return acceptGeneralStreamConnection(destination, openListener, bindListener, optionMap);
    }

    protected IoFuture<StreamConnection> acceptGeneralStreamConnection(final SocketAddress destination, final ChannelListener<? super StreamConnection> openListener, final ChannelListener<? super BoundChannel> bindListener, final OptionMap optionMap) {
        assert destination instanceof InetSocketAddress || destination instanceof LocalSocketAddress;
        try {
            getWorker().checkShutdown();
        } catch (ClosedWorkerException e) {
            return new FailedIoFuture<StreamConnection>(e);
        }
        final FutureResult<StreamConnection> futureResult = new FutureResult<StreamConnection>(this);
        try {
            boolean ok = false;
            final int fd = streamSocket(destination);
            try {
                Native.listen(fd, 1);
                final NativeDescriptor listener = new NativeDescriptor(this, fd) {
                    protected void handleReadReady() {
                        final int nfd = Native.accept(fd);
                        if (nfd == -Native.EAGAIN) {
                            return;
                        }
                        if (nfd < 0) {
                            if (futureResult.setException(Native.exceptionFor(nfd))) {
                                unregister();
                                Native.close(fd);
                            }
                        } else {
                            final NativeStreamConnection connection = destination instanceof LocalSocketAddress ? new UnixConnection(NativeWorkerThread.this, nfd, null) : new TcpConnection(NativeWorkerThread.this, nfd, null);
                            final NativeStreamConduit conduit = connection.getConduit();
                            try {
                                register(conduit);
                            } catch (IOException e) {
                                if (futureResult.setException(e)) {
                                    unregister();
                                    Native.close(fd);
                                }
                                return;
                            }
                            if (futureResult.setResult(connection)) {
                                unregister();
                                Native.close(fd);
                                ChannelListeners.invokeChannelListener(connection, openListener);
                            }
                        }
                    }

                    protected void handleWriteReady() {
                    }
                };
                register(listener);
                try {
                    doResume(listener, true, false);
                    ok = true;
                } finally {
                    if (! ok) {
                        unregister(listener);
                    }
                }
            } finally {
                if (! ok) {
                    Native.close(fd);
                }
            }
        } catch (IOException e) {
            return new FailedIoFuture<>(e);
        }
        return futureResult.getIoFuture();
    }

    protected IoFuture<StreamConnection> openTcpStreamConnection(final InetSocketAddress bindAddress, final InetSocketAddress destinationAddress, final ChannelListener<? super StreamConnection> openListener, final ChannelListener<? super BoundChannel> bindListener, final OptionMap optionMap) {
        return openGeneralStreamConnection(bindAddress, destinationAddress, openListener, bindListener, optionMap);
    }

    protected IoFuture<StreamConnection> openLocalStreamConnection(final LocalSocketAddress bindAddress, final LocalSocketAddress destinationAddress, final ChannelListener<? super StreamConnection> openListener, final ChannelListener<? super BoundChannel> bindListener, final OptionMap optionMap) {
        return openGeneralStreamConnection(bindAddress, destinationAddress, openListener, bindListener, optionMap);
    }

    protected IoFuture<StreamConnection> openGeneralStreamConnection(final SocketAddress bindAddress, final SocketAddress destinationAddress, final ChannelListener<? super StreamConnection> openListener, final ChannelListener<? super BoundChannel> bindListener, final OptionMap optionMap) {
        assert bindAddress instanceof InetSocketAddress && destinationAddress instanceof InetSocketAddress || bindAddress instanceof LocalSocketAddress && destinationAddress instanceof LocalSocketAddress || bindAddress == null && (destinationAddress instanceof LocalSocketAddress || destinationAddress instanceof InetSocketAddress);
        try {
            getWorker().checkShutdown();
        } catch (ClosedWorkerException e) {
            return new FailedIoFuture<StreamConnection>(e);
        }
        boolean ok = false;
        try {
            final int fd = streamSocket(destinationAddress);
            try {
                final NativeStreamConnection connection = destinationAddress instanceof LocalSocketAddress ? new UnixConnection(this, fd, null) : new TcpConnection(this, fd, null);
                final NativeStreamConduit conduit = connection.getConduit();
                register(conduit);
                if (Native.testAndThrowConnect(Native.connect(fd, Native.encodeSocketAddress(destinationAddress))) == 0) {
                    // would block
                    final FutureResult<StreamConnection> futureResult = new FutureResult<StreamConnection>(this);
                    final WriteReadyHandler oldHandler = conduit.getWriteReadyHandler();
                    conduit.setWriteReadyHandler(new WriteReadyHandler() {
                        public void writeReady() {
                            int res = Native.finishConnect(fd);
                            if (res == -Native.EAGAIN) {
                                log.tracef("Connect incomplete");
                                // try again
                                return;
                            }
                            if (res == 0) {
                                log.tracef("Connect complete");
                                // connect finished
                                conduit.suspendWrites();
                                conduit.setWriteReadyHandler(oldHandler);
                                if (futureResult.setResult(connection)) {
                                    ChannelListeners.invokeChannelListener(connection, openListener);
                                }
                                return;
                            }
                            futureResult.setException(Native.exceptionFor(res));
                            unregister(connection.conduit);
                            Native.close(fd);
                        }

                        public void forceTermination() {
                        }

                        public void terminated() {
                        }
                    });
                    conduit.resumeWrites();
                    ok = true;
                    futureResult.addCancelHandler(new Cancellable() {
                        public Cancellable cancel() {
                            if (futureResult.setCancelled()) {
                                unregister(conduit);
                            }
                            return this;
                        }
                    });
                    return futureResult.getIoFuture();
                } else {
                    // connected
                    return new FinishedIoFuture<StreamConnection>(connection);
                }
            } finally {
                if (! ok) {
                    Native.close(fd);
                }
            }
        } catch (IOException e) {
            return new FailedIoFuture<StreamConnection>(e);
        }
    }

    private int streamSocket(final SocketAddress bindAddress) throws IOException {
        if (bindAddress instanceof LocalSocketAddress) {
            return Native.testAndThrow(Native.socketLocalStream());
        } else if (bindAddress instanceof InetSocketAddress) {
            final InetAddress address = ((InetSocketAddress) bindAddress).getAddress();
            if (address instanceof Inet4Address) {
                return Native.testAndThrow(Native.socketTcp());
            } else if (address instanceof Inet6Address) {
                return Native.testAndThrow(Native.socketTcp6());
            }
        }
        throw new IOException("Invalid socket type");
    }

    NativeWorkerThread getNextThread() {
        final NativeWorkerThread[] all = getWorker().getAll();
        final int number = getNumber();
        if (number == all.length - 1) {
            return all[0];
        } else {
            return all[number + 1];
        }
    }

    public ChannelPipe<StreamConnection, StreamConnection> createFullDuplexPipeConnection() throws IOException {
        return super.createFullDuplexPipeConnection();
    }

    public ChannelPipe<StreamConnection, StreamConnection> createFullDuplexPipeConnection(final XnioIoFactory peer) throws IOException {
        return super.createFullDuplexPipeConnection(peer);
    }

    public ChannelPipe<StreamSourceChannel, StreamSinkChannel> createHalfDuplexPipe(final XnioIoFactory peer) throws IOException {
        return super.createHalfDuplexPipe(peer);
    }

    abstract void close();

    abstract void doWakeup();

    abstract void doSelection(long delayTimeMillis);

    public final void interrupt() {
        doWakeup();
        super.interrupt();
    }

    public final void run() {
        try {
            log.tracef("Starting worker thread %s", this);
            Runnable task;
            int oldState;
            for (;;) {
                // run tasks first
                do {
                    task = queue.poll();
                    safeRun(task);
                } while (task != null);
                // all tasks have been run
                oldState = state;
                if ((oldState & SHUTDOWN) != 0) {
                    close();
                    return;
                }

                // perform select
                doSelection(LONGEST_DELAY);
            }
        } finally {
            getWorker().closeResource();
            log.tracef("Shutting down channel thread \"%s\"", this);
        }
    }

    private static void safeRun(final Runnable command) {
        if (command != null) try {
            log.tracef("Running task %s", command);
            command.run();
        } catch (Throwable t) {
            log.taskFailed(command, t);
        }
    }

    public void execute(final Runnable command) {
        if ((state & SHUTDOWN) != 0) {
            throw log.threadExiting();
        }
        queue.add(command);
        if (this != Thread.currentThread()) doWakeup();
    }

    final void shutdown() {
        int oldState;
        do {
            oldState = state;
            if ((oldState & SHUTDOWN) != 0) {
                // idempotent
                return;
            }
        } while (! stateUpdater.compareAndSet(this, oldState, oldState | SHUTDOWN));
        doWakeup();
    }

    abstract void register(NativeDescriptor channel) throws IOException;

    abstract void doResume(NativeDescriptor channel, boolean read, boolean write);

    abstract void unregister(final NativeDescriptor channel);

    public String toString() {
        return String.format("Thread %s (number %d)", getName(), Integer.valueOf(getNumber()));
    }
}
