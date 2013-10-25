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
final class PipeSourceChannel extends NativeSuspendableChannel<PipeSourceChannel> implements StreamSourceChannel{

    PipeSourceChannel(final NativeXnioWorker worker, final int fd, final NativeWorkerThread readThread, final NativeWorkerThread writeThread) throws IOException {
        super(worker, fd, readThread, writeThread);
    }

    public void shutdownReads() throws IOException {
        close();
    }

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
