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

import java.io.IOException;

import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.xnio.OptionMap;
import org.xnio.Xnio;
import org.xnio.XnioWorker;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public abstract class AbstractNativeTest {

    private static final Logger log = Logger.getLogger("org.xnio.native.test");

    static final Xnio XNIO;
    volatile XnioWorker worker;
    private final MultiWaiter multiWaiter = new MultiWaiter();

    static {
        XNIO = Xnio.getInstance("native");
    }

    @Before
    public void createWorker() throws IOException {
        log.trace("Test starting\n===========================================================================================================");
        final MultiWaiter.Ticket ticket = multiWaiter.register("Worker shutdown");
        worker = XNIO.createWorker(null, OptionMap.EMPTY, new Runnable() {
            public void run() {
                ticket.complete();
            }
        });
    }

    @After
    public void destroyWorker() throws InterruptedException {
        final XnioWorker worker = this.worker;
        if (worker != null) {
            worker.shutdown();
            multiWaiter.await();
        }
        log.trace("Test finished\n===========================================================================================================");
    }
}
