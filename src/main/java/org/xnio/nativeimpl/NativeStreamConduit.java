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

package org.xnio.nativeimpl;

import static org.xnio.Bits.*;
import static org.xnio.nativeimpl.Log.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import org.xnio.XnioIoThread;
import org.xnio.channels.ReadTimeoutException;
import org.xnio.channels.StreamSinkChannel;
import org.xnio.channels.StreamSourceChannel;
import org.xnio.channels.WriteTimeoutException;
import org.xnio.conduits.ConduitReadableByteChannel;
import org.xnio.conduits.ConduitWritableByteChannel;
import org.xnio.conduits.Conduits;
import org.xnio.conduits.ReadReadyHandler;
import org.xnio.conduits.StreamSinkConduit;
import org.xnio.conduits.StreamSourceConduit;
import org.xnio.conduits.WriteReadyHandler;
import sun.nio.ch.FileChannelImpl;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
class NativeStreamConduit extends NativeDescriptor implements StreamSourceConduit, StreamSinkConduit {
    private final NativeStreamConnection connection;

    private static final int READ_RESUMED   = 0b00000001;
    private static final int READ_SHUTDOWN  = 0b00000010;

    private static final int WRITE_RESUMED  = 0b00000100;
    private static final int WRITE_SHUTDOWN = 0b00001000;

    private int state;

    private ReadReadyHandler readReadyHandler;
    private WriteReadyHandler writeReadyHandler;

    private int readTimeout;
    private long lastRead;

    private int writeTimeout;
    private long lastWrite;

    NativeStreamConduit(final NativeWorkerThread thread, final int fd, final NativeStreamConnection connection) {
        super(thread, fd);
        this.connection = connection;
    }

    // read methods

    int getAndSetReadTimeout(int newVal) {
        try {
            return readTimeout;
        } finally {
            readTimeout = newVal;
        }
    }

    int getReadTimeout() {
        return readTimeout;
    }

    private void checkReadTimeout(final boolean xfer) throws ReadTimeoutException {
        int timeout = readTimeout;
        if (timeout > 0) {
            if (xfer) {
                lastRead = System.nanoTime();
            } else {
                long lastRead = this.lastRead;
                if (lastRead > 0L && ((System.nanoTime() - lastRead) / 1000000L) > (long) timeout) {
                    throw log.readTimeout();
                }
            }
        }
    }

    public int read(final ByteBuffer dst) throws IOException {
        int res = Native.readSingle(fd, dst);
        checkReadTimeout(res > 0);
        return res;
    }

    public long read(final ByteBuffer[] dsts, final int offs, final int len) throws IOException {
        long res = Native.readScatter(fd, dsts, offs, len);
        checkReadTimeout(res > 0);
        return res;
    }

    public long transferTo(final long position, final long count, FileChannel target) throws IOException {
        target = thread.getWorker().getXnio().unwrapFileChannel(target);
        long res;
        if (Native.HAS_SPLICE && target instanceof FileChannelImpl) {
            res = Native.testAndThrowWrite(Native.spliceToFile(fd, target, position, count));
        } else {
            res = target.transferFrom(new ConduitReadableByteChannel(this), position, count);
        }
        checkReadTimeout(res > 0);
        return res;
    }

    public long transferTo(final long count, final ByteBuffer throughBuffer, final StreamSinkChannel target) throws IOException {
        if (Native.HAS_SPLICE && target instanceof NativeDescriptor) {
            return Native.transfer(fd, count, throughBuffer, ((NativeDescriptor) target).fd);
        } else {
            return Conduits.transfer(this, count, throughBuffer, target);
        }
    }

    public void terminateReads() throws IOException {
        if (connection.readClosed()) try {
            int state = this.state;
            if (allAreClear(state, READ_SHUTDOWN)) {
                this.state = state | READ_SHUTDOWN;
                if (allAreSet(state, WRITE_SHUTDOWN)) {
                    terminate();
                } else {
                    Native.testAndThrow(Native.shutdown(fd, true, false));
                }
            }
        } finally {
            suspendReads();
            readTerminated();
        }
    }

    void readTerminated() {
        final ReadReadyHandler readReadyHandler = this.readReadyHandler;
        if (readReadyHandler != null) try {
            readReadyHandler.terminated();
        } catch (Throwable ignored) {}
    }

    public boolean isReadShutdown() {
        return allAreSet(state, READ_SHUTDOWN);
    }

    public void resumeReads() {
        int state = this.state;
        if (allAreClear(state, READ_RESUMED)) {
            if (Native.EXTRA_TRACE) log.tracef("Resume reads on %s", this);
            this.state = state | READ_RESUMED;
            thread.doResume(this, true, allAreSet(state, WRITE_RESUMED));
        } else {
            if (Native.EXTRA_TRACE) log.tracef("Reads were already resumed on %s", this);
        }
    }

    public void suspendReads() {
        int state = this.state;
        if (allAreSet(state, READ_RESUMED)) {
            if (Native.EXTRA_TRACE) log.tracef("Suspend reads on %s", this);
            this.state = state & ~READ_RESUMED;
            thread.doResume(this, false, allAreSet(state, WRITE_RESUMED));
        } else {
            if (Native.EXTRA_TRACE) log.tracef("Reads were already suspended on %s", this);
        }
    }

    public void wakeupReads() {
        if (Native.EXTRA_TRACE) log.tracef("Wakeup reads on %s", this);
        resumeReads();
        // todo wakeup
    }

    public boolean isReadResumed() {
        return allAreSet(state, READ_RESUMED);
    }

    public void awaitReadable() throws IOException {
        Native.testAndThrow(Native.await2(fd, false));
    }

    public void awaitReadable(final long time, final TimeUnit timeUnit) throws IOException {
        Native.testAndThrow(Native.await3(fd, false, timeUnit.toMillis(time)));
    }

    public void setReadReadyHandler(final ReadReadyHandler handler) {
        readReadyHandler = handler;
    }

    // write methods

    int getAndSetWriteTimeout(int newVal) {
        try {
            return writeTimeout;
        } finally {
            writeTimeout = newVal;
        }
    }

    int getWriteTimeout() {
        return writeTimeout;
    }

    private void checkWriteTimeout(final boolean xfer) throws WriteTimeoutException {
        int timeout = writeTimeout;
        if (timeout > 0) {
            if (xfer) {
                lastWrite = System.nanoTime();
            } else {
                long lastWrite = this.lastWrite;
                if (lastWrite > 0L && ((System.nanoTime() - lastWrite) / 1000000L) > (long) timeout) {
                    throw log.writeTimeout();
                }
            }
        }
    }

    public int write(final ByteBuffer src) throws IOException {
        final int res = Native.writeSingle(fd, src);
        checkWriteTimeout(res > 0);
        return res;
    }

    public long write(final ByteBuffer[] srcs, final int offs, final int len) throws IOException {
        final long res = Native.writeGather(fd, srcs, offs, len);
        checkWriteTimeout(res > 0);
        return res;
    }

    public long transferFrom(FileChannel src, final long position, final long count) throws IOException {
        final long res;
        src = thread.getWorker().getXnio().unwrapFileChannel(src);
        if (Native.HAS_SENDFILE && src instanceof FileChannelImpl) {
            res = Native.testAndThrowRead(Native.sendfile(fd, src, position, count));
        } else {
            res = src.transferTo(position, count, new ConduitWritableByteChannel(this));
        }
        checkWriteTimeout(res > 0);
        return res;
    }

    public long transferFrom(final StreamSourceChannel source, final long count, final ByteBuffer throughBuffer) throws IOException {
        final long res;
        checkWriteTimeout(false);
        if (Native.HAS_SPLICE && source instanceof NativeDescriptor) {
            res = Native.testAndThrowRead(Native.transfer(((NativeDescriptor) source).fd, count, throughBuffer, fd));
        } else {
            res = Conduits.transfer(source, count, throughBuffer, this);
        }
        if (res > 0) checkWriteTimeout(true);
        return res;
    }

    public int writeFinal(final ByteBuffer src) throws IOException {
        return Conduits.writeFinalBasic(this, src);
    }

    public long writeFinal(final ByteBuffer[] srcs, final int offset, final int length) throws IOException {
        return Conduits.writeFinalBasic(this, srcs, offset, length);
    }

    public void terminateWrites() throws IOException {
        if (connection.writeClosed()) try {
            int state = this.state;
            if (allAreClear(state, WRITE_SHUTDOWN)) {
                this.state = state | WRITE_SHUTDOWN;
                if (allAreSet(state, READ_SHUTDOWN)) {
                    terminate();
                } else {
                    Native.testAndThrow(Native.shutdown(fd, false, true));
                }
            }
        } finally {
            suspendWrites();
            writeTerminated();
        }
    }

    public void truncateWrites() throws IOException {
        terminateWrites();
    }

    void writeTerminated() {
        final WriteReadyHandler writeReadyHandler = this.writeReadyHandler;
        if (writeReadyHandler != null) try {
            writeReadyHandler.terminated();
        } catch (Throwable ignored) {}
    }

    public boolean isWriteShutdown() {
        return allAreSet(state, WRITE_SHUTDOWN);
    }

    public void resumeWrites() {
        int state = this.state;
        if (allAreClear(state, WRITE_RESUMED)) {
            if (Native.EXTRA_TRACE) log.tracef("Resume writes on %s", this);
            this.state = state | WRITE_RESUMED;
            thread.doResume(this, allAreSet(state, READ_RESUMED), true);
        } else {
            if (Native.EXTRA_TRACE) log.tracef("Writes were already resumed on %s", this);
        }
    }

    public void suspendWrites() {
        int state = this.state;
        if (allAreSet(state, WRITE_RESUMED)) {
            if (Native.EXTRA_TRACE) log.tracef("Suspend writes on %s", this);
            this.state = state & ~WRITE_RESUMED;
            thread.doResume(this, allAreSet(state, READ_RESUMED), false);
        } else {
            if (Native.EXTRA_TRACE) log.tracef("Writes were already suspended on %s", this);
        }
    }

    public void wakeupWrites() {
        if (Native.EXTRA_TRACE) log.tracef("Wakeup writes on %s", this);
        resumeWrites();
        // todo proper wakeup
    }

    public boolean isWriteResumed() {
        return allAreSet(state, WRITE_RESUMED);
    }

    public void awaitWritable() throws IOException {
        Native.testAndThrow(Native.await2(fd, true));
    }

    public void awaitWritable(final long time, final TimeUnit timeUnit) throws IOException {
        Native.testAndThrow(Native.await3(fd, true, timeUnit.toMillis(time)));
    }

    public void setWriteReadyHandler(final WriteReadyHandler handler) {
        writeReadyHandler = handler;
    }

    public boolean flush() throws IOException {
        if (Native.HAS_CORK) {
            Native.testAndThrow(Native.flushTcpCork(fd));
        }
        return true;
    }

    // General methods

    public XnioIoThread getReadThread() {
        return thread;
    }

    public XnioIoThread getWriteThread() {
        return thread;
    }

    ReadReadyHandler getReadReadyHandler() {
        return readReadyHandler;
    }

    WriteReadyHandler getWriteReadyHandler() {
        return writeReadyHandler;
    }

    protected void handleReadReady() {
        if (allAreSet(state, READ_RESUMED)) {
            final ReadReadyHandler handler = readReadyHandler;
            if (handler == null) {
                if (Native.EXTRA_TRACE) log.tracef("Read ready but no handler on %s", this);
                suspendReads();
            } else {
                try {
                    handler.readReady();
                } catch (Throwable ignored) {
                }
            }
        } else {
            if (Native.EXTRA_TRACE) log.tracef("Read ready but was not resumed on %s", this);
            suspendReads();
        }
    }

    protected void handleWriteReady() {
        if (allAreSet(state, WRITE_RESUMED)) {
            final WriteReadyHandler handler = writeReadyHandler;
            if (handler == null) {
                if (Native.EXTRA_TRACE) log.tracef("Write ready but no handler on %s", this);
                suspendWrites();
            } else {
                try {
                    handler.writeReady();
                } catch (Throwable ignored) {}
            }
        } else {
            if (Native.EXTRA_TRACE) log.tracef("Write ready but was not resumed on %s", this);
            suspendWrites();
        }
    }

    void terminate() throws IOException {
        int state = this.state;
        if (anyAreClear(state, WRITE_SHUTDOWN | READ_SHUTDOWN)) {
            this.state = state | WRITE_SHUTDOWN | READ_SHUTDOWN;
        }
        if (Native.SAFE_GC) try {
            Native.testAndThrow(Native.dup2(Native.DEAD_FD, fd));
        } finally {
            new FdRef<>(this, fd);
        } else {
            // hope for the best
            Native.testAndThrow(Native.close(fd));
        }
    }
}
