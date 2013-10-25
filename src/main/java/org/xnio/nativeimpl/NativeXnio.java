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
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.jboss.logging.Logger;
import org.xnio.OptionMap;
import org.xnio.Version;
import org.xnio.Xnio;
import org.xnio.XnioWorker;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class NativeXnio extends Xnio {

    static final boolean TRACE_CAS;

    static {
        TRACE_CAS = AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
            public Boolean run() {
                return Boolean.valueOf(System.getProperty("xnio.native.trace-cas", "false"));
            }
        }).booleanValue();
    }

    private static final Logger log = Logger.getLogger("org.xnio.native");

    static {
        log.info("XNIO Native Implementation Version " + Version.VERSION);
    }

    public NativeXnio() {
        super("native");
    }

    public XnioWorker createWorker(final ThreadGroup threadGroup, final OptionMap optionMap, final Runnable terminationTask) throws IOException, IllegalArgumentException {
        return new NativeXnioWorker(this, threadGroup, optionMap, terminationTask);
    }
}
