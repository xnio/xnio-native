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
import org.xnio.IoUtils;
import org.xnio.channels.StreamSinkChannel;
import org.xnio.channels.StreamSourceChannel;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class PipeSinkChannel extends NativeSuspendableChannel<PipeSinkChannel> implements StreamSinkChannel {

    PipeSinkChannel(final NativeXnioWorker worker, final int fd, final NativeWorkerThread readThread, final NativeWorkerThread writeThread) throws IOException {
        super(worker, fd, readThread, writeThread);
    }

    public void shutdownWrites() throws IOException {
        close();
    }

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
}
