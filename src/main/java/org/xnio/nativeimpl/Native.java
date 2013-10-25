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

import java.io.FileDescriptor;
import java.io.IOError;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.Arrays;
import org.jboss.logging.Logger;
import org.xnio.Buffers;
import org.xnio.LocalSocketAddress;

import static org.xnio.Bits.allAreSet;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class Native {

    static final Logger log = Logger.getLogger("org.xnio.native");

    // init

    static final int DEAD_FD;
    static final int EAGAIN;
    static final int EINTR;
    static final int UNIX_PATH_LEN;

    static final boolean HAS_EPOLL;
    static final boolean HAS_KQUEUE;
    static final boolean HAS_DEV_POLL;
    static final boolean HAS_PORTS;
    static final boolean HAS_SPLICE;
    static final boolean HAS_SENDFILE;
    static final boolean HAS_CORK;

    static {
        System.loadLibrary("xnio");
        final int[] constants = init();
        if (constants[0] < 0) {
            throw new IOError(exceptionFor(constants[0]));
        }
        try {
            DEAD_FD = testAndThrow(constants[0]);
        } catch (IOException e) {
            throw new IOError(e);
        }
        EAGAIN = constants[1];
        EINTR = constants[2];
        UNIX_PATH_LEN = constants[3];
        HAS_EPOLL = allAreSet(constants[4], (1 << 0));
        HAS_KQUEUE = allAreSet(constants[4], (1 << 1));
        HAS_DEV_POLL = allAreSet(constants[4], (1 << 2));
        HAS_PORTS = allAreSet(constants[4], (1 << 3));
        HAS_SPLICE = allAreSet(constants[4], (1 << 4));
        HAS_SENDFILE = allAreSet(constants[4], (1 << 5));
        HAS_CORK = allAreSet(constants[4], (1 << 6));
    }

    private Native() {}

    private static native int[] init();

    // POSIX-ish

    /**
     * Call the UNIX dup2 system call.
     *
     * @param oldFd the old FD
     * @param newFd the new FD
     * @return the result
     */
    static native int dup2(int oldFd, int newFd);

    /**
     * Call the UNIX dup system call.
     *
     * @param oldFd the old FD
     * @return the new FD or error
     */
    static native int dup(int oldFd);

    /**
     * Close the FD.
     *
     * @param fd the FD to close
     * @return 0 for okay, a negative error code for error
     */
    static native int close(int fd);

    /**
     * Get a string for an error code.
     *
     * @param err a negative or positive error code
     * @return the string
     */
    static native String strError(int err);

    /**
     * Shut down a socket.
     *
     * @param fd the socket FD
     * @param read {@code true} to shut down reads
     * @param write {@code true} to shut down writes
     * @return 0 for okay, a negative error code for error
     */
    static native int shutdown(int fd, boolean read, boolean write);

    static native int await2(final int fd, final boolean writes);

    static native int await3(final int fd, final boolean writes, final long millis);

    static native byte[] getSockName(final int fd);

    static native byte[] getPeerName(final int fd);

    static native long readLong(final int fd);

    static native int readDirect(final int fd, ByteBuffer buffer, int[] posAndLimit);

    static native long readDirectScatter(final int fd, ByteBuffer[] buffers, int offs, int len, int[] pos, int[] limit);

    static native int readHeap(final int fd, byte[] bytes, int[] posAndLimit);

    static native long readHeapScatter(final int fd, byte[][] bytes, int[] pos, int[] limit);

    static native int writeLong(final int fd, final long value);

    static native int writeDirect(final int fd, ByteBuffer buffer, int[] posAndLimit);

    static native long writeDirectGather(final int fd, ByteBuffer[] buffers, int offs, int len, int[] pos, int[] lim);

    static native int writeHeap(final int fd, byte[] bytes, int[] posAndLimit);

    static native long writeHeapGather(final int fd, byte[][] bytes, int[] pos, int[] lim);

    static native int recvDirect(final int fd, ByteBuffer buffer, byte[] srcAddr, byte[] destAddr);

    static native int recvDirectScatter(final int fd, ByteBuffer[] buffers, int offs, int len, int[] pos, int[] lim, byte[] srcAddr, byte[] destAddr);

    static native int recvHeap(final int fd, byte[] bytes, byte[] srcAddr, byte[] destAddr);

    static native int recvHeapScatter(final int fd, byte[] bytes, byte[][] srcAddr, byte[][] destAddr);

    static native int sendDirect(final int fd, ByteBuffer buffer, int[] posAndLimit, byte[] destAddr);

    static native int sendDirectGather(final int fd, ByteBuffer[] buffers, int offs, int len, int[] pos, int[] lim, byte[] destAddr);

    static native int sendHeap(final int fd, byte[] bytes, int[] posAndLimit, byte[] destAddr);

    static native int sendHeapScatter(final int fd, byte[][] bytes, int[] pos, int[] lim, byte[] destAddr);

    static native long xferHeap(final int srcFd, byte[] bytes, int[] posAndLimit, int destFd);

    static native long xferDirect(final int srcFd, ByteBuffer buffer, int[] posAndLimit, int destFd);

    static native long sendfile(final int dest, int src, long offset, long length);

    static native int socketPair(int[] fds);

    static native int socketTcp();

    static native int socketTcp6();

    static native int socketUdp();

    static native int socketUdp6();

    static native int socketLocalStream();

    static native int socketLocalDatagram();

    static native int pipe(int[] fds);

    static native int bind(int fd, byte[] address);

    static native int accept(int fd);

    static native int connect(int fd, byte[] peerAddress);

    static native int listen(int fd, int backlog);

    static native int finishConnect(int fd);

    static native int getOptBroadcast(int fd);

    static native int setOptBroadcast(int fd, boolean enabled);

    static native int getOptDontRoute(int fd);

    static native int setOptDontRoute(int fd, boolean enabled);

    static native int getOptKeepAlive(int fd);

    static native int setOptKeepAlive(int fd, boolean enabled);

    static native int getOptCloseAbort(int fd);

    static native int setOptCloseAbort(int fd, boolean enabled);

    static native int getOptOobInline(int fd);

    static native int setOptOobInline(int fd, boolean enabled);

    static native int getOptReceiveBuffer(int fd);

    static native int setOptReceiveBuffer(int fd, int size);

    static native int getOptReuseAddr(int fd);

    static native int setOptReuseAddr(int fd, boolean enabled);

    static native int getOptSendBuffer(int fd);

    static native int setOptSendBuffer(int fd, int size);

    static native int getOptDeferAccept(int fd);

    static native int setOptDeferAccept(int fd, boolean enabled);

    static native int getOptMaxSegSize(int fd);

    static native int setOptMaxSegSize(int fd, int size);

    static native int getOptTcpNoDelay(int fd);

    static native int setOptTcpNoDelay(int fd, boolean enabled);

    static native int getOptTcpCork(int fd);

    static native int setOptTcpCork(int fd, boolean enabled);

    static native int getOptMulticastTtl(int fd);

    static native int setOptMulticastTtl(int fd, boolean enabled);

    // linux

    static native int eventFD();

    static native int epollCreate();

    static native int epollWait(final int efd, int[] fds, int timeout);

    static native int epollCtlAdd(final int efd, final int fd, boolean read, boolean write, boolean edge);

    static native int epollCtlMod(final int efd, final int fd, boolean read, boolean write, boolean edge);

    static native int epollCtlDel(final int efd, final int fd);

    static native long splice(int src, long srcOffs, int dest, long destOffs, long length, boolean more);

    static native long tee(int src, int dest, long length, boolean more);

    // utilities

    static IOException exceptionFor(int err) {
        String msg = strError(err);
        return new IOException(msg);
    }

    static int testAndThrow(int res) throws IOException {
        if (res < 0) throw exceptionFor(res);
        return res;
    }

    @SuppressWarnings({ "deprecation" })
    static SocketAddress getSocketAddress(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        switch (bytes[0]) {
            case 0: {
                // inet 4
                try {
                    return new InetSocketAddress(InetAddress.getByAddress(Arrays.copyOfRange(bytes, 1, 5)), (((bytes[5]) & 0xff) << 8) | ((bytes[6]) & 0xff));
                } catch (UnknownHostException e) {
                    // ???
                    return null;
                }
            }
            case 1: {
                // inet 6
                try {
                    return new InetSocketAddress(
                            Inet6Address.getByAddress(
                                    null,
                                    Arrays.copyOfRange(bytes, 1, 17),
                                    (bytes[19] & 0xff) << 24 | (bytes[20] & 0xff) << 16 | (bytes[21] & 0xff) << 8 | (bytes[22] & 0xff)
                            ),
                            (((bytes[17]) & 0xff) << 8) | ((bytes[18]) & 0xff));
                } catch (UnknownHostException e) {
                    // ???
                    return null;
                }
            }
            case 2: {
                // local
                int len = bytes[1] & 0xff;
                return new LocalSocketAddress(new String(bytes, 0, 2, len - 2));
            }
            default: {
                return null;
            }
        }
    }

    private static final byte[] INVALID_ADDR = { (byte) 0xff };

    static byte[] encodeSocketAddress(SocketAddress src) {
        if (src == null) {
            return INVALID_ADDR;
        }
        if (src instanceof InetSocketAddress) {
            final InetSocketAddress inet = (InetSocketAddress) src;
            final int port = inet.getPort();
            final InetAddress address = inet.getAddress();
            if (address instanceof Inet4Address) {
                final Inet4Address inet4 = (Inet4Address) address;
                final byte[] bytes = inet4.getAddress();
                final byte[] result = new byte[7];
                result[0] = 0;
                System.arraycopy(bytes, 0, result, 1, 4);
                result[5] = (byte) (port >> 8);
                result[6] = (byte) port;
                return result;
            } else if (address instanceof Inet6Address) {
                final Inet6Address inet6 = (Inet6Address) address;
                final byte[] bytes = inet6.getAddress();
                final byte[] result = new byte[23];
                result[0] = 1;
                System.arraycopy(bytes, 0, result, 1, 16);
                result[17] = (byte) (port >> 8);
                result[18] = (byte) port;
                final int scopeId = inet6.getScopeId();
                result[19] = (byte) (scopeId >> 24);
                result[20] = (byte) (scopeId >> 16);
                result[21] = (byte) (scopeId >> 8);
                result[22] = (byte) scopeId;
                return result;
            } else {
                return INVALID_ADDR;
            }
        } else if (src instanceof LocalSocketAddress) {
            final LocalSocketAddress local = (LocalSocketAddress) src;
            final String name = local.getName();
            final byte[] result = new byte[2 + UNIX_PATH_LEN];
            result[0] = 2;
            if (! encodeTo(name, result, 1)) {
                return INVALID_ADDR;
            }
            return result;
        } else {
            return INVALID_ADDR;
        }
    }

    static boolean encodeTo(String src, byte[] dest, int offs) {
        final int srcLen = src.length();
        try {
            for (int i = 0; i < srcLen; i = src.offsetByCodePoints(i, 1)) {
                int cp = src.codePointAt(i);
                if (cp > 0 && cp <= 0x7f) {
                    // don't accidentally null-terminate the string
                    dest[offs ++] = (byte) cp;
                } else if (cp <= 0x07ff) {
                    dest[offs ++] = (byte)(0xc0 | 0x1f & cp >> 6);
                    dest[offs ++] = (byte)(0x80 | 0x3f & cp);
                } else if (cp <= 0xffff) {
                    dest[offs ++] = (byte)(0xe0 | 0x0f & cp >> 12);
                    dest[offs ++] = (byte)(0x80 | 0x3f & cp >> 6);
                    dest[offs ++] = (byte)(0x80 | 0x3f & cp);
                } else if (cp <= 0x1fffff) {
                    dest[offs ++] = (byte)(0xf0 | 0x07 & cp >> 18);
                    dest[offs ++] = (byte)(0x80 | 0x3f & cp >> 12);
                    dest[offs ++] = (byte)(0x80 | 0x3f & cp >> 6);
                    dest[offs ++] = (byte)(0x80 | 0x3f & cp);
                } else if (cp <= 0x3ffffff) {
                    dest[offs ++] = (byte)(0xf8 | 0x03 & cp >> 24);
                    dest[offs ++] = (byte)(0x80 | 0x3f & cp >> 18);
                    dest[offs ++] = (byte)(0x80 | 0x3f & cp >> 12);
                    dest[offs ++] = (byte)(0x80 | 0x3f & cp >> 6);
                    dest[offs ++] = (byte)(0x80 | 0x3f & cp);
                } else if (cp >= 0) {
                    dest[offs ++] = (byte)(0xfc | 0x01 & cp >> 30);
                    dest[offs ++] = (byte)(0x80 | 0x3f & cp >> 24);
                    dest[offs ++] = (byte)(0x80 | 0x3f & cp >> 18);
                    dest[offs ++] = (byte)(0x80 | 0x3f & cp >> 12);
                    dest[offs ++] = (byte)(0x80 | 0x3f & cp >> 6);
                    dest[offs ++] = (byte)(0x80 | 0x3f & cp);
                } else {
                    return false;
                }
            }
            return true;
        } catch (ArrayIndexOutOfBoundsException e) {
            return false;
        }
    }

    static int doRead(int fd, ByteBuffer buffer) throws IOException {
        if (! buffer.hasRemaining()) {
            return 0;
        }
        if (buffer.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }
        int res;
        final int[] posAndLimit = new int[2];
        if (buffer.isDirect()) {
            posAndLimit[0] = buffer.position();
            posAndLimit[1] = buffer.limit();
            res = readDirect(fd, buffer, posAndLimit);
            if (res == 0) {
                return -1;
            }
            if (res == -EAGAIN) {
                return 0;
            }
            if (res < 0) {
                throw exceptionFor(res);
            }
            if (res > 0) {
                buffer.position(posAndLimit[0]);
            }
        } else if (buffer.hasArray()) {
            final byte[] array = buffer.array();
            final int offs = buffer.arrayOffset();
            posAndLimit[0] = buffer.position() + offs;
            posAndLimit[1] = buffer.limit() + offs;
            res = readHeap(fd, array, posAndLimit);
            if (res == 0) {
                return -1;
            }
            if (res == -EAGAIN) {
                return 0;
            }
            if (res < 0) {
                throw exceptionFor(res);
            }
            if (res > 0) {
                buffer.position(posAndLimit[0] - offs);
            }
        } else {
            final byte[] array = Buffers.take(buffer.duplicate());
            posAndLimit[0] = 0;
            posAndLimit[1] = array.length;
            res = readHeap(fd, array, posAndLimit);
            if (res == 0) {
                return -1;
            }
            if (res == -EAGAIN) {
                return 0;
            }
            if (res < 0) {
                throw exceptionFor(res);
            }
            if (res > 0) {
                buffer.put(array, 0, posAndLimit[0]);
            }
        }
        log.tracef("Read %d bytes from FD %d (single)", res, fd);
        return res;
    }

    static long doRead(final int fd, final ByteBuffer[] dsts, final int offset, final int length) throws IOException {
        if (! Buffers.hasRemaining(dsts, offset, length)) {
            return 0L;
        }
        Buffers.assertWritable(dsts, offset, length);
        final boolean direct = Buffers.isDirect(dsts, offset, length);
        long res = 0L;
        final int[] pos = new int[length];
        final int[] limit = new int[length];
        if (direct) {
            for (int i = 0; i < length; i ++) {
                final ByteBuffer buffer = dsts[offset + i];
                pos[i] = buffer.position();
                limit[i] = buffer.limit();
            }
            res = readDirectScatter(fd, dsts, offset, length, pos, limit);
            if (res == 0) {
                return -1;
            }
            if (res == -EAGAIN) {
                return 0;
            }
            if (res < 0) {
                throw exceptionFor((int) res);
            }
            if (res > 0) {
                for (int i = 0; i < length; i ++) {
                    final ByteBuffer buffer = dsts[offset + i];
                    buffer.position(pos[i]);
                }
            }
        } else {
            final byte[][] bytes = new byte[length][];
            for (int i = 0; i < length; i ++) {
                final ByteBuffer buffer = dsts[offset + i];
                if (buffer.hasArray()) {
                    bytes[i] = buffer.array();
                    final int arrayOffs = buffer.arrayOffset();
                    pos[i] = buffer.position() + arrayOffs;
                    limit[i] = buffer.limit() + arrayOffs;
                } else {
                    bytes[i] = Buffers.take(buffer.duplicate());
                    pos[i] = 0;
                    limit[i] = bytes[i].length;
                }
            }
            res = readHeapScatter(fd, bytes, pos, limit);
            if (res == 0) {
                return -1;
            }
            if (res == -EAGAIN) {
                return 0;
            }
            if (res < 0) {
                throw exceptionFor((int) res);
            }
            if (res > 0) {
                for (int i = 0; i < length; i ++) {
                    final ByteBuffer buffer = dsts[offset + i];
                    if (buffer.hasArray()) {
                        final int arrayOffs = buffer.arrayOffset();
                        buffer.position(pos[i] - arrayOffs);
                    } else {
                        buffer.put(bytes[i], 0, limit[i]);
                    }
                }
            }
        }
        log.tracef("Read %d bytes from FD %d (scatter)", res, fd);
        return res;
    }

    static int doWrite(final int fd, final ByteBuffer buffer) throws IOException {
        if (! buffer.hasRemaining()) {
            return 0;
        }
        final int res;
        final int[] posAndLimit = new int[2];
        if (buffer.isDirect()) {
            posAndLimit[0] = buffer.position();
            posAndLimit[1] = buffer.limit();
            res = writeDirect(fd, buffer, posAndLimit);
            if (res < 0) {
                throw exceptionFor(res);
            }
            if (res > 0) {
                buffer.position(posAndLimit[0]);
            }
        } else if (buffer.hasArray()) {
            final byte[] array = buffer.array();
            final int offs = buffer.arrayOffset();
            posAndLimit[0] = buffer.position() + offs;
            posAndLimit[1] = buffer.limit() + offs;
            res = writeHeap(fd, array, posAndLimit);
            if (res == -EAGAIN) {
                return 0;
            }
            if (res < 0) {
                throw exceptionFor(res);
            }
            if (res > 0) {
                buffer.position(posAndLimit[0] - offs);
            }
        } else {
            final byte[] array = Buffers.take(buffer.duplicate());
            posAndLimit[0] = 0;
            posAndLimit[1] = buffer.remaining();
            res = writeHeap(fd, array, posAndLimit);
            if (res < 0) {
                throw exceptionFor(res);
            }
            if (res > 0) {
                Buffers.skip(buffer, posAndLimit[0]);
            }
        }
        log.tracef("Wrote %d bytes to FD %d (single)", res, fd);
        return res;
    }

    static long doWrite(final int fd, final ByteBuffer[] srcs, final int offset, final int length) throws IOException {
        if (! Buffers.hasRemaining(srcs, offset, length)) {
            return 0L;
        }
        Buffers.assertWritable(srcs, offset, length);
        final boolean direct = Buffers.isDirect(srcs, offset, length);
        long res = 0L;
        final int[] pos = new int[length];
        final int[] limit = new int[length];
        if (direct) {
            for (int i = 0; i < length; i ++) {
                final ByteBuffer buffer = srcs[offset + i];
                pos[i] = buffer.position();
                limit[i] = buffer.limit();
            }
            res = writeDirectGather(fd, srcs, offset, length, pos, limit);
            if (res == -EAGAIN) {
                return 0;
            }
            if (res < 0) {
                throw exceptionFor((int) res);
            }
            if (res > 0) {
                for (int i = 0; i < length; i ++) {
                    final ByteBuffer buffer = srcs[offset + i];
                    buffer.position(pos[i]);
                }
            }
        } else {
            final byte[][] bytes = new byte[length][];
            for (int i = 0; i < length; i ++) {
                final ByteBuffer buffer = srcs[offset + i];
                if (buffer.hasArray()) {
                    bytes[i] = buffer.array();
                    final int arrayOffs = buffer.arrayOffset();
                    pos[i] = buffer.position() + arrayOffs;
                    limit[i] = buffer.limit() + arrayOffs;
                } else {
                    bytes[i] = Buffers.take(buffer.duplicate());
                    pos[i] = 0;
                    limit[i] = buffer.remaining();
                }
            }
            res = writeHeapGather(fd, bytes, pos, limit);
            if (res == -EAGAIN) {
                return 0;
            }
            if (res < 0) {
                throw exceptionFor((int) res);
            }
            if (res > 0) {
                for (int i = 0; i < length; i ++) {
                    final ByteBuffer buffer = srcs[offset + i];
                    if (buffer.hasArray()) {
                        final int arrayOffs = buffer.arrayOffset();
                        buffer.position(pos[i] - arrayOffs);
                    } else {
                        Buffers.skip(buffer, pos[i]);
                    }
                }
            }
        }
        log.tracef("Wrote %d bytes to FD %d (gather)", res, fd);
        return res;
    }

    static long doTransfer(final int srcFd, final long count, final ByteBuffer throughBuffer, final int destFd) {
        throw new UnsupportedOperationException();
    }

    static long doTransfer(final FileDescriptor srcDescriptor, final long position, final long count, final int destFd) {
        throw new UnsupportedOperationException();
    }

    static long doTransfer(final int srcFd, final long position, final long count, final FileDescriptor destDescriptor) {
        throw new UnsupportedOperationException();
    }
}

