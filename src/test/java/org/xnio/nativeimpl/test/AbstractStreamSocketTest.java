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

import static java.lang.Math.max;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
                    log.debug("Connection complete notify");
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
            assertNotNull("No client connection", future.get());
            if (listener != null) listener.doTest(testContext, multiWaiter, serverConnection.get(), clientConnection.get());
            assertFalse("Test was marked as failed, see test output for details", testContext.get());
            multiWaiter.await();
        } finally {
            safeClose(serverConnection.get());
            safeClose(clientConnection.get());
            safeClose(server);
            cleanup();
        }
    }

    protected void cleanup() {
    }

    @Test
    public void testEstablish() throws Throwable {
        establishTest(new MultiWaiter(), null);
    }

    protected void transferTest(final BufferAllocator<ByteBuffer> allocator, final boolean reverse, final int bufferCount) throws Throwable {
        establishTest(new MultiWaiter(), new EstablishTestListener() {
            public void doTest(final TestContext ctxt, final MultiWaiter waiter, final StreamConnection serverConnection, final StreamConnection clientConnection) throws Throwable {
                final ByteBuffer[] buffers1 = new ByteBuffer[max(bufferCount, 1)];
                final ByteBuffer[] buffers2 = new ByteBuffer[max(bufferCount, 1)];
                for (int i = 0; i < buffers1.length; i ++) {
                    buffers1[i] = allocator.allocate(8192);
                    buffers2[i] = allocator.allocate(8192);
                }
                final MultiWaiter.Ticket writeTicket = waiter.register("Channel write");
                final MultiWaiter.Ticket readTicket = waiter.register("Channel read");
                for (final ByteBuffer buffer : buffers1) {
                    Buffers.fill(buffer, 0xEE, buffer.remaining());
                    buffer.flip();
                }
                final ConduitStreamSinkChannel sinkChannel = reverse ? serverConnection.getSinkChannel() : clientConnection.getSinkChannel();
                sinkChannel.setWriteListener(new ChannelListener<ConduitStreamSinkChannel>() {
                    public void handleEvent(final ConduitStreamSinkChannel channel) {
                        try {
                            log.tracef("Writing %d buffers to %s", buffers1.length, channel);
                            final long res = bufferCount == 0 ? channel.write(buffers1[0]) : channel.write(buffers1);
                            log.tracef("Write returned %d", res);
                            if (! Buffers.hasRemaining(buffers1)) {
                                channel.flush();
                                channel.shutdownWrites();
                                channel.flush();
                                writeTicket.complete();
                            }
                        } catch (IOException e) {
                            log.error("Write failed", e);
                            ctxt.markAsFailed();
                            writeTicket.complete();
                        }
                    }
                });
                final ConduitStreamSourceChannel sourceChannel = reverse ? clientConnection.getSourceChannel() : serverConnection.getSourceChannel();
                sourceChannel.setReadListener(new ChannelListener<ConduitStreamSourceChannel>() {
                    public void handleEvent(final ConduitStreamSourceChannel channel) {
                        if (! Buffers.hasRemaining(buffers2)) {
                            final int res;
                            try {
                                res = channel.read(ByteBuffer.allocate(9999));
                                if (res != -1) {
                                    log.errorf("Got read ready notification while there was no space left in the buffer (at least %d bytes)", res);
                                    ctxt.markAsFailed();
                                    readTicket.complete();
                                    return;
                                }
                                // EOF
                                log.debug("Read end-of-file");
                                readTicket.complete();
                            } catch (IOException e) {
                                log.error("Exception at expected end of file", e);
                                ctxt.markAsFailed();
                                readTicket.complete();
                                return;
                            }
                        }
                        try {
                            log.tracef("Reading %d buffers from %s", buffers2.length, channel);
                            final long res = bufferCount == 0 ? channel.read(buffers2[0]) : channel.read(buffers2);
                            log.tracef("Read returned %d", res);
                            int res2 = channel.read(ByteBuffer.allocate(9999));
                            if (res2 != -1) {
                                log.errorf("Got read ready notification while there was no space left in the buffer (at least %d bytes)", res2);
                                ctxt.markAsFailed();
                                readTicket.complete();
                                return;
                            }
                            // EOF
                            log.debug("Read end-of-file");
                            readTicket.complete();
                        } catch (IOException e) {
                            log.error("Read failed", e);
                            ctxt.markAsFailed();
                            readTicket.complete();
                            return;
                        }
                        if (! Buffers.hasRemaining(buffers2)) {
                            try {
                                for (ByteBuffer buffer : buffers2) {
                                    buffer.flip();
                                }
                                final byte[] bytes = new byte[(int) Buffers.remaining(buffers2)];
                                Arrays.fill(bytes, (byte) 0xEE);
                                assertArrayEquals(Buffers.take(buffers2, 0, buffers2.length), bytes);
                            } catch (Throwable t) {
                                log.error("Assertion(s) failed", t);
                                ctxt.markAsFailed();
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
    public void testTransferDirect0() throws Throwable {
        transferTest(BufferAllocator.DIRECT_BYTE_BUFFER_ALLOCATOR, false, 0);
        transferTest(BufferAllocator.DIRECT_BYTE_BUFFER_ALLOCATOR, true, 0);
    }

    @Test
    public void testTransferHeap0() throws Throwable {
        transferTest(BufferAllocator.BYTE_BUFFER_ALLOCATOR, false, 0);
        transferTest(BufferAllocator.BYTE_BUFFER_ALLOCATOR, true, 0);
    }

    @Test
    public void testTransferDirect1() throws Throwable {
        transferTest(BufferAllocator.DIRECT_BYTE_BUFFER_ALLOCATOR, false, 1);
        transferTest(BufferAllocator.DIRECT_BYTE_BUFFER_ALLOCATOR, true, 1);
    }

    @Test
    public void testTransferHeap1() throws Throwable {
        transferTest(BufferAllocator.BYTE_BUFFER_ALLOCATOR, false, 1);
        transferTest(BufferAllocator.BYTE_BUFFER_ALLOCATOR, true, 1);
    }

    @Test
    public void testTransferDirect2() throws Throwable {
        transferTest(BufferAllocator.DIRECT_BYTE_BUFFER_ALLOCATOR, false, 2);
        transferTest(BufferAllocator.DIRECT_BYTE_BUFFER_ALLOCATOR, true, 2);
    }

    @Test
    public void testTransferHeap2() throws Throwable {
        transferTest(BufferAllocator.BYTE_BUFFER_ALLOCATOR, false, 2);
        transferTest(BufferAllocator.BYTE_BUFFER_ALLOCATOR, true, 2);
    }

    static class MixedAllocator extends AtomicBoolean implements BufferAllocator<ByteBuffer> {
        private static final MixedAllocator INSTANCE = new MixedAllocator();

        public ByteBuffer allocate(final int size) throws IllegalArgumentException {
            boolean o, n;
            do {
                o = get();
            } while (! compareAndSet(o, !o));
            return o ? BYTE_BUFFER_ALLOCATOR.allocate(size) : DIRECT_BYTE_BUFFER_ALLOCATOR.allocate(size);
        }
    }

    @Test
    public void testTransferMixed2() throws Throwable {
        transferTest(MixedAllocator.INSTANCE, false, 2);
        transferTest(MixedAllocator.INSTANCE, true, 2);
    }

    @Test
    public void testTransferDirect3() throws Throwable {
        transferTest(BufferAllocator.DIRECT_BYTE_BUFFER_ALLOCATOR, false, 3);
        transferTest(BufferAllocator.DIRECT_BYTE_BUFFER_ALLOCATOR, true, 3);
    }

    @Test
    public void testTransferHeap3() throws Throwable {
        transferTest(BufferAllocator.BYTE_BUFFER_ALLOCATOR, false, 3);
        transferTest(BufferAllocator.BYTE_BUFFER_ALLOCATOR, true, 3);
    }

    @Test
    public void testTransferMixed3() throws Throwable {
        transferTest(MixedAllocator.INSTANCE, false, 3);
        transferTest(MixedAllocator.INSTANCE, true, 3);
    }

    @Test
    public void testTransferDirect4() throws Throwable {
        transferTest(BufferAllocator.DIRECT_BYTE_BUFFER_ALLOCATOR, false, 4);
        transferTest(BufferAllocator.DIRECT_BYTE_BUFFER_ALLOCATOR, true, 4);
    }

    @Test
    public void testTransferHeap4() throws Throwable {
        transferTest(BufferAllocator.BYTE_BUFFER_ALLOCATOR, false, 4);
        transferTest(BufferAllocator.BYTE_BUFFER_ALLOCATOR, true, 4);
    }

    @Test
    public void testTransferMixed4() throws Throwable {
        transferTest(MixedAllocator.INSTANCE, false, 4);
        transferTest(MixedAllocator.INSTANCE, true, 4);
    }
}
