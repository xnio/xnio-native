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

import static org.xnio.IoUtils.safeClose;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.jboss.logging.Logger;
import org.junit.Test;
import org.xnio.ChannelListener;
import org.xnio.IoFuture;
import org.xnio.OptionMap;
import org.xnio.StreamConnection;
import org.xnio.channels.AcceptingChannel;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public abstract class AbstractStreamSocketTest extends AbstractNativeTest {

    private static final Logger log = Logger.getLogger("org.xnio.native.test");

    protected abstract SocketAddress getServerAddress() throws UnknownHostException;

    @Test
    public void testEstablish() throws Throwable {
        System.out.println("Creating server");
        final List<Throwable> problems = Collections.synchronizedList(new ArrayList<Throwable>());
        final MultiWaiter multiWaiter = new MultiWaiter();
        final SocketAddress serverAddress = getServerAddress();
        final MultiWaiter.Ticket sar = multiWaiter.register("Server complete");
        final AtomicReference<StreamConnection> clientConnection = new AtomicReference<>();
        final AtomicReference<StreamConnection> serverConnection = new AtomicReference<>();
        final AcceptingChannel<StreamConnection> server = worker.createStreamConnectionServer(serverAddress, new ChannelListener<AcceptingChannel<StreamConnection>>() {
            public void handleEvent(final AcceptingChannel<StreamConnection> channel) {
                log.info("Accept ready notify");
                try {
                    final StreamConnection connection = channel.accept();
                    if (connection != null) {
                        log.infof("Accepted %s", connection);
                        serverConnection.set(connection);
                        sar.complete();
                    } else {
                        log.info("Spurious wakeup");
                    }
                } catch (IOException e) {
                    log.errorf(e, "Adding problem");
                    problems.add(e);
                    sar.complete();
                }
            }
        }, OptionMap.EMPTY);
        server.resumeAccepts();
        final MultiWaiter.Ticket cc = multiWaiter.register("Client connected");
        final IoFuture<StreamConnection> future = worker.openStreamConnection(serverAddress, new ChannelListener<StreamConnection>() {
            public void handleEvent(final StreamConnection channel) {
                log.info("Connection complete notify");
                clientConnection.set(channel);
                cc.complete();
            }
        }, OptionMap.EMPTY);
        future.addNotifier(new IoFuture.HandlingNotifier<StreamConnection, MultiWaiter.Ticket>() {
            public void handleCancelled(final MultiWaiter.Ticket attachment) {
                log.info("Connection cancelled");
                attachment.complete();
            }

            public void handleFailed(final IOException exception, final MultiWaiter.Ticket attachment) {
                problems.add(exception);
                log.info("Connection failed", exception);
                attachment.complete();
            }
        }, cc);
        multiWaiter.await();
        try {
            if (clientConnection.get() != null) clientConnection.get().close();
            if (serverConnection.get() != null) serverConnection.get().close();
            server.close();
        } finally {
            safeClose(clientConnection.get());
            safeClose(serverConnection.get());
            safeClose(server);
        }
        synchronized (problems) {
            for (Throwable problem : problems) {
                throw problem;
            }
        }
    }
}
