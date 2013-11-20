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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.xnio.IoUtils.safeClose;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.jboss.logging.Logger;
import org.junit.Test;
import org.xnio.BufferAllocator;
import org.xnio.Buffers;
import org.xnio.ChannelListener;
import org.xnio.IoFuture;
import org.xnio.OptionMap;
import org.xnio.StreamConnection;
import org.xnio.channels.AcceptingChannel;
import org.xnio.conduits.ConduitStreamSinkChannel;
import org.xnio.conduits.ConduitStreamSourceChannel;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public abstract class AbstractStreamSocketTest extends AbstractNativeTest {

    private static final Logger log = Logger.getLogger("org.xnio.native.test");

    protected abstract SocketAddress getServerAddress() throws UnknownHostException;

    protected final class TestContext extends AtomicBoolean {

        public void markAsFailed() {
            set(true);
        }
    }

    protected interface EstablishTestListener {
        void doTest(TestContext ctxt, MultiWaiter waiter, StreamConnection serverConnection, StreamConnection clientConnection) throws Throwable;
    }

    protected void establishTest(MultiWaiter multiWaiter, EstablishTestListener listener) throws Throwable {
        log.debug("Creating server");
        final TestContext testContext = new TestContext();
        final MultiWaiter.Ticket sar = multiWaiter.register("Server complete");
        final AtomicReference<StreamConnection> serverConnection = new AtomicReference<>();
        final AtomicReference<StreamConnection> clientConnection = new AtomicReference<>();
        final SocketAddress serverAddress = getServerAddress();
        final AcceptingChannel<StreamConnection> server = worker.createStreamConnectionServer(serverAddress, new ChannelListener<AcceptingChannel<StreamConnection>>() {
            public void handleEvent(final AcceptingChannel<StreamConnection> channel) {
                log.debug("Accept ready notify");
                try {
                    final StreamConnection connection = channel.accept();
                    if (connection != null) {
                        log.debugf("Accepted %s", connection);
                        serverConnection.set(connection);
                        channel.close();
                        sar.complete();
                    } else {
                        log.debug("Spurious wakeup");
                    }
                } catch (IOException e) {
                    log.errorf(e, "Accept or close failed");
                    safeClose(channel);
                    testContext.markAsFailed();
                    sar.complete();
                }
            }
        }, OptionMap.EMPTY);
        try {
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
                    log.info("Connection failed", exception);
                    testContext.markAsFailed();
                    attachment.complete();
                }
            }, cc);
            multiWaiter.await();
            assertFalse("Test was marked as failed during setup, see test output for details", testContext.get());
            if (listener != null) listener.doTest(testContext, multiWaiter, serverConnection.get(), clientConnection.get());
            assertFalse("Test was marked as failed, see test output for details", testContext.get());
            multiWaiter.await();
        } finally {
            safeClose(serverConnection.get());
            safeClose(clientConnection.get());
            safeClose(server);
        }
    }

    @Test
    public void testEstablish() throws Throwable {
        establishTest(new MultiWaiter(), null);
    }

    protected void transferTest(final BufferAllocator<ByteBuffer> allocator) throws Throwable {
        establishTest(new MultiWaiter(), new EstablishTestListener() {
            public void doTest(final TestContext ctxt, final MultiWaiter waiter, final StreamConnection serverConnection, final StreamConnection clientConnection) throws Throwable {
                final ByteBuffer buffer1 = allocator.allocate(8192);
                final ByteBuffer buffer2 = allocator.allocate(8192);
                final MultiWaiter.Ticket writeTicket = waiter.register("Channel write");
                final MultiWaiter.Ticket readTicket = waiter.register("Channel read");
                Buffers.fill(buffer1, 0xEE, buffer1.remaining());
                buffer1.flip();
                final ConduitStreamSinkChannel sinkChannel = clientConnection.getSinkChannel();
                sinkChannel.setWriteListener(new ChannelListener<ConduitStreamSinkChannel>() {
                    public void handleEvent(final ConduitStreamSinkChannel channel) {
                        try {
                            log.tracef("Writing %s to %s", buffer1, channel);
                            final int res = channel.write(buffer1);
                            log.tracef("Write returned %d", res);
                            if (! buffer1.hasRemaining()) {
                                writeTicket.complete();
                                channel.suspendWrites();
                            }
                        } catch (IOException e) {
                            log.error("Write failed", e);
                            ctxt.markAsFailed();
                        }
                    }
                });
                final ConduitStreamSourceChannel sourceChannel = serverConnection.getSourceChannel();
                sourceChannel.setReadListener(new ChannelListener<ConduitStreamSourceChannel>() {
                    public void handleEvent(final ConduitStreamSourceChannel channel) {
                        if (! buffer2.hasRemaining()) {
                            log.error("Got read ready notification while there was no space left in the buffer");
                            ctxt.markAsFailed();
                            return;
                        }
                        try {
                            log.tracef("Reading %s from %s", buffer2, channel);
                            final int res = channel.read(buffer2);
                            log.tracef("Read returned %d", res);
                        } catch (IOException e) {
                            log.error("Read failed", e);
                            ctxt.markAsFailed();
                            return;
                        }
                        if (! buffer2.hasRemaining()) {
                            try {
                                buffer2.flip();
                                final byte[] bytes = new byte[8192];
                                Arrays.fill(bytes, (byte) 0xEE);
                                assertArrayEquals(Buffers.take(buffer2), bytes);
                            } catch (Throwable t) {
                                log.error("Assertion(s) failed", t);
                                ctxt.markAsFailed();
                            } finally {
                                readTicket.complete();
                            }
                        }
                    }
                });
                log.debug("Transfer test: resuming writes on client connection");
                sinkChannel.resumeWrites();
                log.debug("Transfer test: resuming reads on server connection");
                sourceChannel.resumeReads();
            }
        });
    }

    @Test
    public void testTransferDirect() throws Throwable {
        transferTest(BufferAllocator.DIRECT_BYTE_BUFFER_ALLOCATOR);
    }

    @Test
    public void testTransferHeap() throws Throwable {
        transferTest(BufferAllocator.BYTE_BUFFER_ALLOCATOR);
    }
}
