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

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import org.xnio.OptionMap;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.management.XnioProviderMXBean;
import org.xnio.management.XnioServerMXBean;
import org.xnio.management.XnioWorkerMXBean;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class NativeXnio extends Xnio {

    static {
        Log.log.greeting(Version.getVersionString());
    }

    public NativeXnio() {
        super("native");
        register(new XnioProviderMXBean() {
            public String getName() {
                return "native";
            }

            public String getVersion() {
                return Version.getVersionString();
            }
        });
    }

    public XnioWorker createWorker(final ThreadGroup threadGroup, final OptionMap optionMap, final Runnable terminationTask) throws IOException, IllegalArgumentException {
        final NativeXnioWorker worker = new NativeXnioWorker(this, threadGroup, optionMap, terminationTask);
        worker.start();
        return worker;
    }

    protected FileChannel unwrapFileChannel(final FileChannel src) {
        return super.unwrapFileChannel(src);
    }

    protected static Closeable register(XnioWorkerMXBean mbean) {
        return Xnio.register(mbean);
    }

    protected static Closeable register(XnioServerMXBean mbean) {
        return Xnio.register(mbean);
    }
}
