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
        server.invokeAcceptHandler();
    }

    protected void handleWriteReady() {
    }

    void resume() {
        final NativeWorkerThread thread = this.thread;
        if (thread == currentThread()) {
            if (! stopped && server.resumed) thread.doResume(this, true, false);
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
        if (thread == currentThread()) {
            if (! stopped && ! server.resumed) thread.doResume(this, false, false);
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
        if (count-- <= low && tokenCount != 0 && stopped) {
            stopped = false;
            if (server.resumed) {
                thread.doResume(this, true, false);
            }
        }
    }

    void setTokenCount(final int newCount) {
        NativeWorkerThread workerThread = thread;
        if (workerThread == currentThread()) {
            if (tokenCount == 0) {
                tokenCount = newCount;
                if (count <= low && stopped) {
                    stopped = false;
                    if (server.resumed) {
                        thread.doResume(this, true, false);
                    }
                }
                return;
            }
            workerThread = workerThread.getNextThread();
        }
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
        final int number = workerThread.getNumber();
        if (workerThread == currentThread()) {
            tokenCount = newCount;
            if (newCount == 0) {
                stopped = true;
                thread.doResume(this, false, false);
            }
        } else {
            workerThread.execute(new Runnable() {
                public void run() {
                    server.getHandle(number).initializeTokenCount(newCount);
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
            setThreadNewCount(thread.getNextThread(), server.getTokenConnectionCount());
        }
        if (++count >= high || tokenCount == 0) {
            stopped = true;
            thread.doResume(this, false, false);
        }
        return true;
    }

    public void executeSetTask(final int high, final int low) {
        final NativeWorkerThread thread = this.thread;
        if (thread == currentThread()) {
            this.high = high;
            this.low = low;
            if (count >= high && ! stopped) {
                stopped = true;
                suspend();
            } else if (count <= low && stopped) {
                stopped = false;
                if (server.resumed) resume();
            }
        } else {
            thread.execute(new Runnable() {
                public void run() {
                    executeSetTask(high, low);
                }
            });
        }
    }
}
