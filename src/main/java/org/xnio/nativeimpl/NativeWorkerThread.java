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
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.jboss.logging.Logger;
import org.xnio.XnioExecutor;

import static java.lang.System.nanoTime;
import static org.xnio.Bits.*;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
abstract class NativeWorkerThread extends Thread implements XnioExecutor {

    private static final long LONGEST_DELAY = 9223372036853L;

    private static final Logger log = Logger.getLogger("org.xnio.native");

    private final NativeXnioWorker worker;

    private volatile int state = 1;

    private static final int CLOSE_REQ_FLAG = 1 << 31;
    private static final int CLOSED_FLAG = 1 << 30;
    private static final int RESOURCE_MASK = intBitMask(0, 29);

    private final Object lock = new Object();

    private final Queue<Runnable> workQueue = new ArrayDeque<Runnable>();
    private final SortedSet<TimeKey> delayQueue = new TreeSet<TimeKey>();

    private final FdMap channelMap = new FdMap();

    private static final AtomicIntegerFieldUpdater<NativeWorkerThread> stateUpdater = AtomicIntegerFieldUpdater.newUpdater(NativeWorkerThread.class, "state");
    private final boolean writeThread;

    NativeWorkerThread(final NativeXnioWorker worker, final String name, final ThreadGroup group, final long stackSize, final boolean writeThread) {
        super(group, null, name, stackSize);
        this.worker = worker;
        this.writeThread = writeThread;
    }

    private boolean tryEnter() {
        int oldState;
        do {
            oldState = state;
            if (allAreSet(oldState, CLOSE_REQ_FLAG)) {
                return false;
            }
        } while (! stateUpdater.compareAndSet(this, oldState, oldState + 1));
        return true;
    }

    private boolean tryTerminate() {
        int oldState;
        do {
            oldState = state;
            if (anyAreSet(oldState, RESOURCE_MASK) || allAreClear(oldState, CLOSE_REQ_FLAG)) {
                return false;
            }
        } while (! stateUpdater.compareAndSet(this, oldState, CLOSED_FLAG | CLOSE_REQ_FLAG));
        return true;
    }

    final void exit() {
        if (stateUpdater.decrementAndGet(this) == (CLOSED_FLAG | CLOSE_REQ_FLAG)) {
            close();
        }
    }

    abstract void close();

    abstract void doWakeup();

    abstract void doGetEvents(Queue<NativeRawChannel<?>> readyQueue, long delayTimeMillis);

    final NativeRawChannel<?> channelFor(int fd) {
        return channelMap.get(fd);
    }

    public final void interrupt() {
        wakeup();
        super.interrupt();
    }

    final void wakeup() {
        if (tryEnter()) try {
            doWakeup();
        } finally {
            exit();
        }
    }

    public final void run() {
        try {
            log.tracef("Starting worker thread %s", this);
            Runnable task;
            Iterator<TimeKey> iterator;
            final Queue<NativeRawChannel<?>> readyQueue = new ArrayDeque<NativeRawChannel<?>>();
            long delayTime = Long.MAX_VALUE;
            for (;;) {
                // run tasks first
                do {
                    synchronized (lock) {
                        task = workQueue.poll();
                        if (task == null) {
                            iterator = delayQueue.iterator();
                            delayTime = Long.MAX_VALUE;
                            if (iterator.hasNext()) {
                                final long now = nanoTime();
                                do {
                                    final TimeKey key = iterator.next();
                                    if (key.deadline <= now) {
                                        workQueue.add(key.command);
                                        iterator.remove();
                                    } else {
                                        delayTime = key.deadline - now;
                                        // the rest are in the future
                                        break;
                                    }
                                } while (iterator.hasNext());
                            }
                            task = workQueue.poll();
                        }
                    }
                    safeRun(task);
                } while (task != null);

                // check for shutdown
                if (tryTerminate()) {
                    return;
                }

                // execute selection
                doGetEvents(readyQueue, 1L + delayTime / 10000000L);
                NativeRawChannel<?> channel;
                while ((channel = readyQueue.poll()) != null) {
                    channel.notifyReady(this);
                }
            }
        } finally {
            worker.closeResource();
            log.tracef("Shutting down channel thread \"%s\"", this);
            exit();
        }
    }

    private static void safeRun(final Runnable command) {
        if (command != null) try {
            log.tracef("Running task %s", command);
            command.run();
        } catch (Throwable t) {
            log.error("Task failed on channel thread", t);
        }
    }

    public void execute(final Runnable command) {
        if (! tryEnter()) {
            throw new RejectedExecutionException("Thread is terminating");
        }
        try {
            synchronized (lock) {
                workQueue.add(command);
            }
            if (this != Thread.currentThread()) wakeup();
        } finally {
            exit();
        }
    }

    public XnioExecutor.Key executeAfter(final Runnable command, final long time, final TimeUnit unit) {
        return executeAfter(command, unit.toMillis(time));
    }

    XnioExecutor.Key executeAfter(final Runnable command, final long time) {
        if (! tryEnter()) {
            throw new RejectedExecutionException("Thread is terminating");
        }
        try {
            if (time <= 0) {
                execute(command);
                return XnioExecutor.Key.IMMEDIATE;
            }
            final long deadline = nanoTime() + Math.min(time, LONGEST_DELAY) * 1000000L;
            final TimeKey key = new TimeKey(deadline, command);
            synchronized (lock) {
                final SortedSet<TimeKey> queue = delayQueue;
                queue.add(key);
                if (this != Thread.currentThread()) {
                    if (queue.iterator().next() == key) {
                        // we're the next one up; poke the selector to update its delay time
                        wakeup();
                    }
                }
                return key;
            }
        } finally {
            exit();
        }
    }

    abstract void doRegister(NativeRawChannel<?> channel) throws IOException;

    final void register(NativeRawChannel<?> channel) throws IOException {
        if (tryEnter()) try {
            log.tracef("Adding channel %s to %s", channel, this);
            if (channelMap.put(channel) == null) {
                doRegister(channel);
            }
        } finally {
            exit();
        } else {
            throw new ClosedChannelException();
        }
    }

    abstract void doResume(int fd, boolean read, boolean write);

    final void resumeFd(final int fd, final boolean read, final boolean write) {
        if (tryEnter()) try {
            log.tracef("Modifying set for %d read=%s write=%s", Integer.valueOf(fd), Boolean.valueOf(read), Boolean.valueOf(write));
            doResume(fd, read, write);
        } finally {
            exit();
        }
    }

    final void shutdown() {
        int oldState;
        do {
            oldState = state;
            if (allAreSet(oldState, CLOSE_REQ_FLAG)) {
                return;
            }
        } while (! stateUpdater.compareAndSet(this, oldState, 1 + (oldState | CLOSE_REQ_FLAG)));
        try {
            doWakeup();
        } finally {
            exit();
        }
    }

    final void unregister(final NativeRawChannel<?> channel) {
        channelMap.remove(channel);
    }

    boolean isWriteThread() {
        return writeThread;
    }

    final class TimeKey implements XnioExecutor.Key, Comparable<TimeKey> {
        private final long deadline;
        private final Runnable command;

        TimeKey(final long deadline, final Runnable command) {
            this.deadline = deadline;
            this.command = command;
        }

        public boolean remove() {
            synchronized (lock) {
                return delayQueue.remove(this);
            }
        }

        public int compareTo(final TimeKey o) {
            return (int) Math.signum(deadline - o.deadline);
        }
    }
}
