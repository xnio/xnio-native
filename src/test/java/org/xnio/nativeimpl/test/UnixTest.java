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

import java.io.File;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import org.junit.After;
import org.junit.Before;
import org.xnio.LocalSocketAddress;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class UnixTest extends AbstractStreamSocketTest {

    private static final String SOCK_PATH = "/tmp/xnio-test.sock";

    protected SocketAddress getServerAddress() throws UnknownHostException {
        return new LocalSocketAddress("/tmp/xnio-test.sock");
    }

    @Before
    @After
    public void clearSocket() {
        new File(SOCK_PATH).delete();
    }
}
