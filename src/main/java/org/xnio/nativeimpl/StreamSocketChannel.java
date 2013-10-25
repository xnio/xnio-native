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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.jboss.logging.Logger;
import org.xnio.IoUtils;
import org.xnio.channels.ConnectedStreamChannel;
import org.xnio.channels.StreamSinkChannel;
import org.xnio.channels.StreamSourceChannel;

import static org.xnio.nativeimpl.NativeXnio.TRACE_CAS;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
abstract class StreamSocketChannel<C extends StreamSocketChannel<C>> extends SocketChannel<C> implements ConnectedStreamChannel {
    private static final Logger log = Logger.getLogger("org.xnio.native");
    private static final String FQCN = StreamSocketChannel.class.getName();

    StreamSocketChannel(final NativeXnioWorker worker, final int fd, final NativeWorkerThread readThread, final NativeWorkerThread writeThread, final NativeAcceptChannel<?> acceptChannel) throws IOException {
        super(worker, fd, readThread, writeThread, acceptChannel);
    }

    public void shutdownReads() throws IOException {
        boolean closed = false;
        switch (closeReads()) {
            case 2: closed = true;
            case 1: try {
                if (TRACE_CAS && log.isTraceEnabled()) {
                    log.logf(FQCN, Logger.Level.TRACE, null, "Shutting down reads on %s", this);
                }
                int res = Native.shutdown(fd, true, false);
                if (res < 0) throw Native.exceptionFor(res);
            } finally {
                if (closed) invokeCloseListener();
            }
            case 0: break;
        }
    }

    public void shutdownWrites() throws IOException {
        boolean closed = false;
        switch (closeWrites()) {
            case 2: closed = true;
            case 1: try {
                if (TRACE_CAS && log.isTraceEnabled()) {
                    log.logf(FQCN, Logger.Level.TRACE, null, "Shutting down writes on %s", this);
                }
                int res = Native.shutdown(fd, false, true);
                if (res < 0) throw Native.exceptionFor(res);
            } finally {
                if (closed) invokeCloseListener();
            }
            case 0: break;
        }
    }

    protected final void closeAction() throws IOException {
        Native.testAndThrow(Native.shutdown(fd, true, true));
    }

    // stream write ops

    public int write(final ByteBuffer src) throws IOException {
        return Native.doWrite(fd, src);
    }

    public long write(final ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }

    public long write(final ByteBuffer[] srcs, final int offset, final int length) throws IOException {
        return Native.doWrite(fd, srcs, offset, length);
    }

    public long transferFrom(final FileChannel src, final long position, final long count) throws IOException {
        return src.transferTo(position, count, this);
    }

    public long transferFrom(final StreamSourceChannel source, final long count, final ByteBuffer throughBuffer) throws IOException {
        if (source instanceof NativeRawChannel) {
            final NativeRawChannel<?> fileDescriptorChannel = (NativeRawChannel<?>) source;
            return Native.doTransfer(fileDescriptorChannel.fd, count, throughBuffer, fd);
        } else {
            return IoUtils.transfer(source, count, throughBuffer, this);
        }
    }

    // stream read ops

    public int read(final ByteBuffer dst) throws IOException {
        return Native.doRead(fd, dst);
    }

    public long read(final ByteBuffer[] dsts) throws IOException {
        return read(dsts, 0, dsts.length);
    }

    public long read(final ByteBuffer[] dsts, final int offset, final int length) throws IOException {
        return Native.doRead(fd, dsts, offset, length);
    }

    public long transferTo(final long position, final long count, final FileChannel target) throws IOException {
        return target.transferFrom(this, position, count);
    }

    public long transferTo(final long count, final ByteBuffer throughBuffer, final StreamSinkChannel target) throws IOException {
        if (target instanceof NativeRawChannel) {
            final NativeRawChannel<?> fileDescriptorChannel = (NativeRawChannel<?>) target;
            return Native.doTransfer(fd, count, throughBuffer, fileDescriptorChannel.fd);
        } else {
            return IoUtils.transfer(this, count, throughBuffer, target);
        }
    }
}
