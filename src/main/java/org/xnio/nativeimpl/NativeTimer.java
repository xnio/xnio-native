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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.xnio.XnioExecutor;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class NativeTimer extends NativeDescriptor implements XnioExecutor.Key {
    private final Runnable task;
    // todo if this works use something better than this
    private final AtomicBoolean canRun = new AtomicBoolean(true);

    NativeTimer(final NativeWorkerThread thread, final int fd, final Runnable task) {
        super(thread, fd);
        this.task = task;
    }

    public boolean remove() {
        if (canRun.getAndSet(false)) {
            thread.execute(new Runnable() {
                public void run() {
                    unregister();
                    try {
                        close();
                    } catch (IOException ignored) {
                    }
                }
            });
            return false;
        } else {
            return true;
        }
    }

    protected void handleReadReady() {
        if (canRun.getAndSet(false)) {
            unregister();
            try {
                close();
            } catch (IOException ignored) {
            }
            try {
                task.run();
            } catch (Throwable ignored) {
            }
        }
    }

    protected void handleWriteReady() {
        throw new IllegalStateException();
    }
}
