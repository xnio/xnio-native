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
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
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

    static final boolean SAFE_GC;
    static final boolean EXTRA_TRACE;

    static {
        try {
            System.loadLibrary("xnio");
        } catch (Error error) {
            log.errorf("Failed to load XNIO from [%s]: %s", System.getProperty("java.library.path"), error);
            throw error;
        }
        final int[] constants = init();
        try {
            DEAD_FD = testAndThrow(constants[0]);
        } catch (IOException e) {
            throw new IOError(e);
        }
        EAGAIN          = constants[1];
        EINTR           = constants[2];
        UNIX_PATH_LEN   = constants[3];
        HAS_EPOLL       = allAreSet(constants[4], 0b0000001);
        HAS_KQUEUE      = allAreSet(constants[4], 0b0000010);
        HAS_DEV_POLL    = allAreSet(constants[4], 0b0000100);
        HAS_PORTS       = allAreSet(constants[4], 0b0001000);
        HAS_SPLICE      = allAreSet(constants[4], 0b0010000);
        HAS_SENDFILE    = allAreSet(constants[4], 0b0100000);
        HAS_CORK        = allAreSet(constants[4], 0b1000000);
        SAFE_GC = AccessController.doPrivileged(new BooleanPropertyAction("xnio.native.safe-gc")).booleanValue();
        EXTRA_TRACE = AccessController.doPrivileged(new BooleanPropertyAction("xnio.native.extra-trace")).booleanValue();
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

    // read

    static native long readLong(final int fd);

    static native int readD(final int fd, ByteBuffer b1, int p1, int l1);

    static native long readDD(final int fd, ByteBuffer b1, int p1, int l1, ByteBuffer b2, int p2, int l2);

    static native long readDDD(final int fd, ByteBuffer b1, int p1, int l1, ByteBuffer b2, int p2, int l2, ByteBuffer b3, int p3, int l3);

    static native int readH(final int fd, byte[] b1, int p1, int l1);

    static native long readHH(final int fd, byte[] b1, int p1, int l1, byte[] b2, int p2, int l2);

    static native long readHHH(final int fd, byte[] b1, int p1, int l1, byte[] b2, int p2, int l2, byte[] b3, int p3, int l3);

    // slower
    static native long readMisc(final int fd, ByteBuffer[] buffers, int offs, int len);

    static int readSingle(final int fd, ByteBuffer buf1) throws IOException {
        if (buf1.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }
        final int cnt;
        final int pos1 = buf1.position();
        if (buf1.isDirect()) {
            cnt = testAndThrowNB(readD(fd, buf1, pos1, buf1.limit()));
        } else {
            cnt = testAndThrowNB(readH(fd, buf1.array(), pos1 + buf1.arrayOffset(), buf1.limit() + buf1.arrayOffset()));
        }
        if (cnt > 0) {
            buf1.position(pos1 + cnt);
        }
        return cnt == 0 ? -1 : cnt;
    }

    static long readScatter(final int fd, ByteBuffer[] buffers, int offs, int len) throws IOException {
        if (len <= 0L) {
            return 0L;
        }
        final ByteBuffer buf1 = buffers[offs];
        if (buf1.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }
        final int pos1 = buf1.position();
        final int lim1 = buf1.limit();
        final boolean dir1 = buf1.isDirect();
        if (len == 1) {
            final int cnt;
            if (dir1) {
                cnt = testAndThrowNB(readD(fd, buf1, pos1, lim1));
            } else {
                final int off1 = buf1.arrayOffset();
                cnt = testAndThrowNB(readH(fd, buf1.array(), pos1 + off1, lim1 + off1));
            }
            if (cnt > 0) {
                buf1.position(pos1 + cnt);
            }
            return cnt;
        }
        final ByteBuffer buf2 = buffers[offs + 1];
        if (buf2.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }
        final int pos2 = buf2.position();
        final int lim2 = buf2.limit();
        final boolean dir2 = buf2.isDirect();
        if (len == 2) {
            final long cnt;
            if (dir1 && dir2) {
                cnt = testAndThrowNB(readDD(fd, buf1, pos1, lim1, buf2, pos2, lim2));
            } else if (!dir1 && !dir2) {
                final int off1 = buf1.arrayOffset();
                final int off2 = buf2.arrayOffset();
                cnt = testAndThrowNB(readHH(fd, buf1.array(), pos1 + off1, lim1 + off1, buf2.array(), pos2 + off2, lim2 + off2));
            } else {
                cnt = readMisc(fd, buffers, offs, len);
            }
            if (cnt > 0L) {
                if (pos1 + cnt <= lim1) {
                    buf1.position(pos1 + (int) cnt);
                } else {
                    buf1.position(lim1);
                    buf2.position(pos2 + (int) (cnt - (lim1 - pos1)));
                }
            }
            return cnt;
        }
        final ByteBuffer buf3 = buffers[offs + 2];
        if (buf3.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }
        final int pos3 = buf3.position();
        final int lim3 = buf3.limit();
        final boolean dir3 = buf3.isDirect();
        if (len == 3) {
            final long cnt;
            if (dir1 && dir2 && dir3) {
                cnt = testAndThrowNB(readDDD(fd, buf1, pos1, lim1, buf2, pos2, lim2, buf3, pos3, lim3));
            } else if (!dir1 && !dir2 && !dir3) {
                final int off1 = buf1.arrayOffset();
                final int off2 = buf2.arrayOffset();
                final int off3 = buf3.arrayOffset();
                cnt = testAndThrowNB(readHHH(fd, buf1.array(), pos1 + off1, lim1 + off1, buf2.array(), pos2 + off2, lim2 + off2, buf3.array(), pos3 + off3, lim3 + off3));
            } else {
                cnt = readMisc(fd, buffers, offs, len);
            }
            if (cnt > 0L) {
                if (pos1 + cnt <= lim1) {
                    buf1.position(pos1 + (int) cnt);
                } else if (pos1 + cnt - lim1 <= lim2) {
                    buf1.position(lim1);
                    buf2.position(pos2 + (int) (cnt - (lim1 - pos1)));
                } else {
                    buf1.position(lim1);
                    buf2.position(lim2);
                    buf3.position(pos3 + (int) (cnt - (lim1 - pos1 + lim2 - pos2)));
                }
            }
            return cnt;
        }
        for (int i = 3; i < len; i ++) {
            if (buffers[i].isReadOnly()) {
                throw new ReadOnlyBufferException();
            }
        }
        final long cnt = readMisc(fd, buffers, offs, len);
        Buffers.trySkip(buffers, offs, len, cnt);
        return cnt;
    }

    // write

    static native int writeLong(final int fd, final long value);

    static native int writeD(final int fd, ByteBuffer b1, int p1, int l1);

    static native long writeDD(final int fd, ByteBuffer b1, int p1, int l1, ByteBuffer b2, int p2, int l2);

    static native long writeDDD(final int fd, ByteBuffer b1, int p1, int l1, ByteBuffer b2, int p2, int l2, ByteBuffer b3, int p3, int l3);

    static native int writeH(final int fd, byte[] b1, int p1, int l1);

    static native long writeHH(final int fd, byte[] b1, int p1, int l1, byte[] b2, int p2, int l2);

    static native long writeHHH(final int fd, byte[] b1, int p1, int l1, byte[] b2, int p2, int l2, byte[] b3, int p3, int l3);

    // slower
    static native long writeMisc(final int fd, ByteBuffer[] buffers, int offs, int len);

    static native int flushTcpCork(final int fd);

    static int writeSingle(final int fd, ByteBuffer buf1) throws IOException {
        final int cnt;
        final int pos1 = buf1.position();
        if (buf1.isDirect()) {
            cnt = testAndThrowNB(writeD(fd, buf1, pos1, buf1.limit()));
        } else {
            cnt = testAndThrowNB(writeH(fd, buf1.array(), pos1 + buf1.arrayOffset(), buf1.limit() + buf1.arrayOffset()));
        }
        if (cnt > 0) {
            buf1.position(pos1 + cnt);
        }
        return cnt == 0 ? -1 : cnt;
    }

    static long writeGather(final int fd, ByteBuffer[] buffers, int offs, int len) throws IOException {
        if (len <= 0L) {
            return 0L;
        }
        final ByteBuffer buf1 = buffers[offs];
        final int pos1 = buf1.position();
        final int lim1 = buf1.limit();
        final boolean dir1 = buf1.isDirect();
        if (len == 1) {
            final int cnt;
            if (dir1) {
                cnt = testAndThrowNB(writeD(fd, buf1, pos1, lim1));
            } else {
                final int off1 = buf1.arrayOffset();
                cnt = testAndThrowNB(writeH(fd, buf1.array(), pos1 + off1, lim1 + off1));
            }
            if (cnt > 0) {
                buf1.position(pos1 + cnt);
            }
            return cnt;
        }
        final ByteBuffer buf2 = buffers[offs + 1];
        final int pos2 = buf2.position();
        final int lim2 = buf2.limit();
        final boolean dir2 = buf2.isDirect();
        if (len == 2) {
            final long cnt;
            if (dir1 && dir2) {
                cnt = testAndThrowNB(writeDD(fd, buf1, pos1, lim1, buf2, pos2, lim2));
            } else if (!dir1 && !dir2) {
                final int off1 = buf1.arrayOffset();
                final int off2 = buf2.arrayOffset();
                cnt = testAndThrowNB(writeHH(fd, buf1.array(), pos1 + off1, lim1 + off1, buf2.array(), pos2 + off2, lim2 + off2));
            } else {
                cnt = writeMisc(fd, buffers, offs, len);
            }
            if (cnt > 0L) {
                if (pos1 + cnt <= lim1) {
                    buf1.position(pos1 + (int) cnt);
                } else {
                    buf1.position(lim1);
                    buf2.position(pos2 + (int) (cnt - (lim1 - pos1)));
                }
            }
            return cnt;
        }
        final ByteBuffer buf3 = buffers[offs + 2];
        final int pos3 = buf3.position();
        final int lim3 = buf3.limit();
        final boolean dir3 = buf3.isDirect();
        if (len == 3) {
            final long cnt;
            if (dir1 && dir2 && dir3) {
                cnt = testAndThrowNB(writeDDD(fd, buf1, pos1, lim1, buf2, pos2, lim2, buf3, pos3, lim3));
            } else if (!dir1 && !dir2 && !dir3) {
                final int off1 = buf1.arrayOffset();
                final int off2 = buf2.arrayOffset();
                final int off3 = buf3.arrayOffset();
                cnt = testAndThrowNB(writeHHH(fd, buf1.array(), pos1 + off1, lim1 + off1, buf2.array(), pos2 + off2, lim2 + off2, buf3.array(), pos3 + off3, lim3 + off3));
            } else {
                cnt = writeMisc(fd, buffers, offs, len);
            }
            if (cnt > 0L) {
                if (pos1 + cnt <= lim1) {
                    buf1.position(pos1 + (int) cnt);
                } else if (pos1 + cnt - lim1 <= lim2) {
                    buf1.position(lim1);
                    buf2.position(pos2 + (int) (cnt - (lim1 - pos1)));
                } else {
                    buf1.position(lim1);
                    buf2.position(lim2);
                    buf3.position(pos3 + (int) (cnt - (lim1 - pos1 + lim2 - pos2)));
                }
            }
            return cnt;
        }
        final long cnt = writeMisc(fd, buffers, offs, len);
        Buffers.trySkip(buffers, offs, len, cnt);
        return cnt;
    }


    // receive

    static native int recvDirect(final int fd, ByteBuffer buffer, byte[] srcAddr, byte[] destAddr);

    static native int recvHeap(final int fd, byte[] bytes, int offs, int len, int pos, int lim, byte[] srcAddr, byte[] destAddr);

    static native int recvMisc(final int fd, ByteBuffer[] buffers, int offs, int len, byte[] srcAddr, byte[] destAddr);

    // send

    static native int sendDirect(final int fd, ByteBuffer buffer, int[] posAndLimit, byte[] destAddr);

    static native int sendHeap(final int fd, byte[] bytes, int offs, int len, byte[] destAddr);

    static native int sendMisc(final int fd, ByteBuffer[] buffers, int offs, int len, byte[] destAddr);

    // transfer

    static native long xferHeap(final int srcFd, byte[] bytes, int[] posAndLimit, int destFd);

    static native long xferDirect(final int srcFd, ByteBuffer buffer, int[] posAndLimit, int destFd);

    static native long sendfile(final int dest, FileChannel src, long offset, long length);

    // util

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

    // options

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

    static native int getOptMulticastTtl(int fd);

    static native int setOptMulticastTtl(int fd, boolean enabled);

    // linux

    static final int EPOLL_FLAG_READ    = 0b0000_0001;
    static final int EPOLL_FLAG_WRITE   = 0b0000_0010;
    static final int EPOLL_FLAG_EDGE    = 0b0000_0100;

    static native int eventFD();

    static native int epollCreate();

    static native int epollWait(final int efd, long[] events, int timeout);

    static native int epollCtlAdd(final int efd, final int fd, final int flags, final int id);

    static native int epollCtlMod(final int efd, final int fd, final int flags, final int id);

    static native int epollCtlDel(final int efd, final int fd);

    static native int createTimer(final int seconds, final int nanos);

    static native long spliceToFile(int src, FileChannel dest, long destOffs, long length);

    static native long transfer(final int srcFd, final long count, final ByteBuffer throughBuffer, final int destFd);

    static native long tee(int src, int dest, long length, boolean more);

    // utilities

    static IOException exceptionFor(int err) {
        String msg = strError(err);
        return new IOException(msg);
    }

    static int testAndThrowNB(int res) throws IOException {
        return res == -EAGAIN ? 0 : testAndThrow(res);
    }

    static long testAndThrowNB(long res) throws IOException {
        return res == -EAGAIN ? 0L : testAndThrow(res);
    }

    static int testAndThrowEOF(int res) throws IOException {
        if (res < 0) throw exceptionFor(res);
        return res == 0 ? -1 : res;
    }

    static long testAndThrowEOF(long res) throws IOException {
        if (res < 0) throw exceptionFor((int) res);
        return res == 0L ? -1L : res;
    }

    static int testAndThrow(int res) throws IOException {
        if (res < 0) throw exceptionFor(res);
        return res;
    }

    static long testAndThrow(long res) throws IOException {
        if (res < 0) throw exceptionFor((int) res);
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
            return encodeSocketAddress((LocalSocketAddress) src);
        } else {
            return INVALID_ADDR;
        }
    }

    static byte[] encodeSocketAddress(LocalSocketAddress local) {
        final String name = local.getName();
        final byte[] result = new byte[2 + UNIX_PATH_LEN];
        result[0] = 2;
        if (! encodeTo(name, result, 1)) {
            return INVALID_ADDR;
        }
        return result;
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

    static class BooleanPropertyAction implements PrivilegedAction<Boolean> {

        private final String key;

        BooleanPropertyAction(final String key) {
            this.key = key;
        }

        public Boolean run() {
            return Boolean.valueOf(System.getProperty(key, "false"));
        }
    }
}

