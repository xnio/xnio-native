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

import java.io.IOError;
import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.xnio.Bits.*;
import static org.xnio.nativeimpl.Log.epollLog;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class EPollWorkerThread extends NativeWorkerThread {

    // EPoll FD
    private final int epfd;
    // event FD for waking up epoll
    private final int evfd;
    // select this many at a time; low 32 bits is ID, high 32 bits is flags
    private final long[] events = new long[128];
    // map of epoll IDs to files
    private final EPollMap epollMap = new EPollMap();
    // epoll ID counter; 0 is reserved for the epoll event FD
    private int epollId = 1;

    EPollWorkerThread(final NativeXnioWorker worker, final int threadNumber, final String name, final ThreadGroup group, final long stackSize) throws IOException {
        super(worker, threadNumber, name, group, stackSize);
        boolean ok = false;
        epfd = Native.testAndThrow(Native.epollCreate());
        try {
            evfd = Native.testAndThrow(Native.eventFD());
            try {
                Native.testAndThrow(Native.epollCtlAdd(epfd, evfd, Native.EPOLL_FLAG_READ | Native.EPOLL_FLAG_EDGE, 0));
                ok = true;
            } finally {
                if (! ok) {
                    Native.close(evfd);
                }
            }
        } finally {
            if (! ok) {
                Native.close(epfd);
            }
        }
    }

    void close() {
        assert this == currentThread();
        epollLog.tracef("Closing %s", this);
        Native.close(evfd);
        Native.close(epfd);
    }

    void doWakeup() {
        assert this == currentThread();
        epollLog.tracef("Waking up %s", this);
        Native.writeLong(evfd, 1L);
    }

    void doSelection(final long delayTimeMillis) {
        assert this == currentThread();
        final long[] events = this.events;
        final int epfd = this.epfd;
        final int evfd = this.evfd;
        int cnt;
        try {
            int res;
            do {
                res = Native.epollWait(epfd, events, (int) Math.min((long) Integer.MAX_VALUE, delayTimeMillis));
            } while (res == -Native.EINTR);
            cnt = Native.testAndThrow(res);
        } catch (IOException e) {
            epollLog.trace("Problem reading epoll", e);
            throw new IOError(e);
        }
        long event;
        int id;
        NativeDescriptor channel;
        EPollRegistration reg;
        while (cnt > 0) {
            for (int i = 0; i < cnt; i ++) {
                event = events[i];
                id = (int) (event >> 32L);
                if (id == 0) {
                    // wakeup
                    epollLog.tracef("Consuming wakeup on %s", this);
                    Native.readLong(evfd);
                } else {
                    reg = epollMap.get(id);
                    if (reg != null) {
                        channel = reg.channel;
                        if (channel != null) {
                            epollLog.tracef("Channel %s is ready", channel);
                            if (allAreSet(event, Native.EPOLL_FLAG_WRITE)) {
                                channel.handleWriteReady();
                            }
                            if (allAreSet(event, Native.EPOLL_FLAG_READ)) {
                                channel.handleReadReady();
                            }
                        }
                    } else {
                        epollLog.tracef("Ghost epoll for ID %d; ignoring but may cause a spin", id);
                    }
                }
                try {
                    cnt = Native.testAndThrow(Native.epollWait(epfd, events, 0));
                } catch (IOException e) {
                    epollLog.trace("Problem reading epoll", e);
                    throw new IOError(e);
                }
            }
        }
    }

    public Key executeAfter(final Runnable command, final long time, final TimeUnit unit) {
        final int seconds = (int) Math.min(unit.toSeconds(time), (long)Integer.MAX_VALUE);
        final int nanos = (int) (unit.toNanos(time) % 1000000000L);
        final int fd = Native.createTimer(seconds, nanos);
        if (fd < 0) {
            throw new RejectedExecutionException("Not enough resources to create timer");
        }
        boolean ok = false;
        try {
            final NativeTimer timer = new NativeTimer(this, fd, command);
            try {
                if (this == currentThread()) {
                    register(timer);
                    doResume(fd, true, false);
                } else {
                    execute(new Runnable() {
                        public void run() {
                            try {
                                register(timer);
                                doResume(fd, true, false);
                            } catch (IOException e) {
                                // this is a problem... punt!
                                timer.handleReadReady();
                            }
                        }
                    });
                }
                ok = true;
                return timer;
            } catch (IOException e) {
                throw new RejectedExecutionException("Not enough resources to create timer");
            }
        } finally {
            if (! ok) {
                Native.close(fd);
            }
        }
    }

    void register(final NativeDescriptor channel) throws IOException {
        int id;
        boolean ok = false;
        EPollRegistration registration = null;
        try {
            synchronized (epollMap) {
                epollLog.tracef("Registering %s", channel);
                while ((id = epollId++) == 0 || epollMap.containsKey(id));
                registration = new EPollRegistration(id, channel);
                channel.setId(id);
                epollMap.add(registration);
            }
            Native.testAndThrow(Native.epollCtlAdd(epfd, channel.fd, 0, id));
            ok = true;
        } finally {
            if (! ok) synchronized (epollMap) {
                epollMap.remove(registration);
            }
        }
        if (currentThread() != this) {
            doWakeup();
        }
    }

    void doResume(final int fd, final boolean read, final boolean write) {
        assert this == currentThread();
        epollLog.tracef("Resuming read=%s write=%s on %d", read, write, fd);
        try {
            Native.testAndThrow(Native.epollCtlMod(epfd, fd, (read ? Native.EPOLL_FLAG_READ : 0) | (write ? Native.EPOLL_FLAG_WRITE : 0) | Native.EPOLL_FLAG_EDGE));
        } catch (IOException e) {
            epollLog.warnf(e, "Resume failed on FD %d (%s, %s)", fd, read, write);
        }
    }

    void unregister(final NativeDescriptor channel) {
        synchronized (epollMap) {
            final EPollRegistration registration = epollMap.removeKey(channel.id);
            assert registration.channel == channel; // if not, we got a problem
            Native.epollCtlDel(epfd, channel.fd);
            // no need to wake up; worst outcome is a false positive which is no different
        }
    }
}
