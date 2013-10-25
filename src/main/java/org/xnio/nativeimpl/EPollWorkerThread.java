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
import java.util.Queue;
import org.jboss.logging.Logger;

import static org.xnio.nativeimpl.Native.*;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class EPollWorkerThread extends NativeWorkerThread {

    private static final Logger log = Logger.getLogger("org.xnio.native.epoll");

    private final int epfd;
    private final int evfd;
    private final int[] fds = new int[512];

    EPollWorkerThread(final NativeXnioWorker worker, final String name, final ThreadGroup group, final long stackSize, final boolean writeThread) throws IOException {
        super(worker, name, group, stackSize, writeThread);
        boolean ok = false;
        epfd = Native.testAndThrow(Native.epollCreate());
        try {
            evfd = Native.testAndThrow(Native.eventFD());
            try {
                Native.testAndThrow(Native.epollCtlAdd(epfd, evfd, true, false, false));
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
        log.tracef("Closing %s", this);
        Native.close(evfd);
        Native.close(epfd);
    }

    void doWakeup() {
        log.tracef("Waking up %s", this);
        Native.writeLong(evfd, 1L);
    }

    void doGetEvents(final Queue<NativeRawChannel<?>> readyQueue, final long delayTimeMillis) {
        final int[] fds = this.fds;
        final int epfd = this.epfd;
        final int evfd = this.evfd;
        int cnt;
        try {
            int res;
            do {
                res = Native.epollWait(epfd, fds, (int) Math.min((long) Integer.MAX_VALUE, delayTimeMillis));
            } while (res == -EINTR);
            cnt = Native.testAndThrow(res);
        } catch (IOException e) {
            log.trace("Problem reading epoll", e);
            throw new IOError(e);
        }
        int fd;
        NativeRawChannel<?> channel;
        while (cnt > 0) {
            for (int i = 0; i < cnt; i ++) {
                fd = fds[i];
                if (fd == evfd) {
                    // wakeup
                    log.tracef("Consuming wakeup on %s", this);
                    Native.readLong(fd);
                } else {
                    channel = channelFor(fd);
                    if (channel != null) {
                        log.tracef("Channel %s is ready", channel);
                        readyQueue.add(channel);
                    }
                }
                try {
                    cnt = Native.testAndThrow(Native.epollWait(epfd, fds, 0));
                } catch (IOException e) {
                    log.trace("Problem reading epoll", e);
                    throw new IOError(e);
                }
            }
        }
    }

    void doRegister(final NativeRawChannel<?> channel) throws IOException {
        log.tracef("Registering %s", channel);
        Native.testAndThrow(Native.epollCtlAdd(epfd, channel.fd, false, false, true));
    }

    void doResume(final int fd, final boolean read, final boolean write) {
        log.tracef("Resuming read=%s write=%s on %d", read, write, fd);
        try {
            Native.testAndThrow(Native.epollCtlMod(epfd, fd, read, write, true));
        } catch (IOException e) {
            log.warnf(e, "Resume failed on FD %d (%s, %s)", fd, read, write);
        }
    }
}
