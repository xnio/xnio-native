/*
 * JBoss, Home of Professional Open Source
 *
 * Copyright 2013 Red Hat, Inc. and/or its affiliates.
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

import static java.lang.Thread.currentThread;
import static org.xnio.nativeimpl.Log.log;

/**
* @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
*/
final class AcceptChannelHandle extends NativeDescriptor {

    private final Runnable freeTask;
    private final NativeAcceptChannel<?> server;
    private int count;
    private int low;
    private int high;
    private int tokenCount = -1;
    private boolean stopped;

    AcceptChannelHandle(final NativeAcceptChannel<?> server, final int fd, final NativeWorkerThread thread, final int low, final int high) {
        super(thread, fd);
        this.server = server;
        this.low = low;
        this.high = high;
        freeTask = new Runnable() {
            public void run() {
                freeConnection();
            }
        };
    }

    protected void handleReadReady() {
        if (Native.EXTRA_TRACE) log.tracef("Invoke accept handler on %s", this);
        server.invokeAcceptHandler();
    }

    protected void handleWriteReady() {
    }

    void resume() {
        final NativeWorkerThread thread = this.thread;
        if (Native.EXTRA_TRACE) log.tracef("Request resume on %s", this);
        if (thread == currentThread()) {
            if (! stopped && server.resumed) {
                thread.doResume(this, true, false, false);
                if (Native.EXTRA_TRACE) log.tracef("Complete resume on %s", this);
            } else {
                if (Native.EXTRA_TRACE) log.tracef("Resume cancelled on %s", this);
            }
        } else {
            thread.execute(new Runnable() {
                public void run() {
                    resume();
                }
            });
        }
    }

    void suspend() {
        final NativeWorkerThread thread = this.thread;
        if (Native.EXTRA_TRACE) log.tracef("Request suspend on %s", this);
        if (thread == currentThread()) {
            if (stopped || ! server.resumed) {
                thread.doResume(this, false, false, false);
                if (Native.EXTRA_TRACE) log.tracef("Complete suspend on %s", this);
            } else {
                if (Native.EXTRA_TRACE) log.tracef("Suspend cancelled on %s", this);
            }
        } else {
            thread.execute(new Runnable() {
                public void run() {
                    suspend();
                }
            });
        }
    }

    void channelClosed() {
        final NativeWorkerThread thread = this.thread;
        if (thread == currentThread()) {
            freeConnection();
        } else {
            thread.execute(freeTask);
        }
    }

    void freeConnection() {
        assert currentThread() == thread;
        if (Native.EXTRA_TRACE) log.tracef("Freeing connection on %s", this);
        if (count-- <= low && tokenCount != 0 && stopped) {
            stopped = false;
            if (server.resumed) {
                if (Native.EXTRA_TRACE) log.tracef("Freeing connection on %s -> resume", this);
                thread.doResume(this, true, false, false);
            }
        }
    }

    void setTokenCount(final int newCount) {
        NativeWorkerThread workerThread = thread;
        if (workerThread == currentThread()) {
            if (Native.EXTRA_TRACE) log.tracef("Set token count on %s (tokenCount: %d)", this, tokenCount);
            if (tokenCount == 0) {
                tokenCount = newCount;
                if (count <= low && stopped) {
                    stopped = false;
                    if (server.resumed) {
                        if (Native.EXTRA_TRACE) log.tracef("Accept resumed on %s (count: %d, tokenCount: %d)", this, count, tokenCount);
                        thread.doResume(this, true, false, false);
                    }
                }
                return;
            }
            workerThread = workerThread.getNextThread();
        }
        if (Native.EXTRA_TRACE) log.tracef("Delegating token set from %s to %s", this, workerThread.getName());
        setThreadNewCount(workerThread, newCount);
    }

    private void setThreadNewCount(final NativeWorkerThread workerThread, final int newCount) {
        final int number = workerThread.getNumber();
        workerThread.execute(new Runnable() {
            public void run() {
                server.getHandle(number).setTokenCount(newCount);
            }
        });
    }

    void initializeTokenCount(final int newCount) {
        NativeWorkerThread workerThread = thread;
        if (workerThread == currentThread()) {
            tokenCount = newCount;
            if (newCount == 0) {
                stopped = true;
                if (Native.EXTRA_TRACE) log.tracef("Token count set on %s (stopped; no tokens)", this);
                workerThread.doResume(this, false, false, false);
            } else {
                if (Native.EXTRA_TRACE) log.tracef("Token count set on %s (initial tokenCount: %d)", this, newCount);
            }
        } else {
            workerThread.execute(new Runnable() {
                public void run() {
                    initializeTokenCount(newCount);
                }
            });
        }
    }

    boolean getConnection() {
        assert currentThread() == thread;
        if (stopped) {
            return false;
        }
        if (tokenCount != -1 && --tokenCount == 0) {
            if (Native.EXTRA_TRACE) log.tracef("Passing token on %s", this);
            setThreadNewCount(thread.getNextThread(), server.getTokenConnectionCount());
        }
        if (++count >= high || tokenCount == 0) {
            stopped = true;
            if (Native.EXTRA_TRACE) log.tracef("Accept stopped on %s (count: %d, tokenCount: %d)", this, count, tokenCount);
            thread.doResume(this, false, false, false);
        }
        return true;
    }

    void executeSetTask(final int high, final int low) {
        final NativeWorkerThread thread = this.thread;
        if (thread == currentThread()) {
            if (Native.EXTRA_TRACE) log.tracef("Setting low/high to %d/%d on %s", low, high, this);
            this.high = high;
            this.low = low;
            if (count >= high && ! stopped) {
                stopped = true;
                if (Native.EXTRA_TRACE) log.tracef("Setting low/high -> suspend on %s", this);
                suspend();
            } else if (count <= low && stopped) {
                stopped = false;
                if (server.resumed) {
                    if (Native.EXTRA_TRACE) log.tracef("Setting low/high -> resume on %s", this);
                    resume();
                }
            }
        } else {
            thread.execute(new Runnable() {
                public void run() {
                    executeSetTask(high, low);
                }
            });
        }
    }

    int getConnectionCount() {
        assert currentThread() == this.thread;
        return count;
    }
}
