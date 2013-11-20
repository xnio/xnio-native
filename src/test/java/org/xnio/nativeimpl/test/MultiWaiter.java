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

package org.xnio.nativeimpl.test;

import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;
import static org.xnio.Bits.allAreSet;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class MultiWaiter {
    private final Object lock = new Object();

    private final String[] descriptions = new String[64];
    private long outstanding;

    public void await() throws InterruptedException {
        synchronized (lock) {
            long outstanding = this.outstanding;
            if (outstanding == 0L) {
                return;
            }
            long remainingTime = TimeUnit.SECONDS.toNanos(6);
            long start = System.nanoTime();
            long now;
            for (;;) {
                lock.wait(remainingTime / 1_000_000L, (int) (remainingTime % 1_000_000L));
                outstanding = this.outstanding;
                if (outstanding == 0L) {
                    return;
                }
                now = System.nanoTime();
                remainingTime -= now - start;
                if (remainingTime <= 0L) {
                    StringBuilder failureMessage = new StringBuilder();
                    failureMessage.append("The following are incomplete:\n");
                    for (int i = 0; i < 64; i ++) {
                        if (allAreSet(outstanding, 1L << i)) {
                            failureMessage.append("    ").append(descriptions[i]).append('\n');
                        }
                    }
                    throw new IllegalStateException(failureMessage.toString());
                }
                start = now;
            }
        }
    }

    public Ticket register(String description) {
        synchronized (lock) {
            int i = numberOfTrailingZeros(lowestOneBit(~outstanding));
            if (i == 64) {
                throw new IllegalStateException("Too many concurrent registrations");
            }
            outstanding |= 1L << i;
            descriptions[i] = description;
            return new Ticket(i);
        }
    }

    public final class Ticket extends AtomicBoolean {
        final int index;

        Ticket(final int index) {
            this.index = index;
        }

        public void complete() {
            if (! getAndSet(true)) {
                synchronized (lock) {
                    if ((outstanding &= ~(1L << index)) == 0L) {
                        lock.notifyAll();
                    }
                }
            }
        }
    }
}
