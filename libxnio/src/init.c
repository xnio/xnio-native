
#include "xnio.h"

#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/tcp.h>

static int deadFD();

extern int kqueue(void) weak;
#ifdef __linux__
extern int epoll_create(int) weak;
extern ssize_t splice(int, loff_t *, int, loff_t *, size_t, unsigned int) weak;
extern ssize_t sendfile(int, int, off_t *, size_t) weak;
#endif

jfieldID Buffer_pos;
jfieldID Buffer_lim;

jfieldID ByteBuffer_array;
jfieldID ByteBuffer_offset;

jfieldID FileChannelImpl_fd;
jfieldID FileDescriptor_fd;

JNIEXPORT jintArray JNICALL xnio_native(init)(JNIEnv *env, jclass clazz, jobject preserve) {
    jclass Buffer = (*env)->FindClass(env, "java/nio/Buffer");
    if (! Buffer) {
        return 0;
    }
    jclass ByteBuffer = (*env)->FindClass(env, "java/nio/ByteBuffer");
    if (! ByteBuffer) {
        return 0;
    }
    jclass FileChannelImpl = (*env)->FindClass(env, "sun/nio/ch/FileChannelImpl");
    if (! FileChannelImpl) {
        return 0;
    }
    jclass FileDescriptor = (*env)->FindClass(env, "java/io/FileDescriptor");
    if (! FileDescriptor) {
        return 0;
    }
    Buffer_pos = (*env)->GetFieldID(env, Buffer, "position", "I");
    if (! Buffer_pos) {
        return 0;
    }
    Buffer_lim = (*env)->GetFieldID(env, Buffer, "limit", "I");
    if (! Buffer_lim) {
        return 0;
    }
    ByteBuffer_array = (*env)->GetFieldID(env, ByteBuffer, "hb", "[B");
    if (! ByteBuffer_array) {
        return 0;
    }
    ByteBuffer_offset = (*env)->GetFieldID(env, ByteBuffer, "offset", "I");
    if (! ByteBuffer_offset) {
        return 0;
    }
    FileChannelImpl_fd = (*env)->GetFieldID(env, FileChannelImpl, "fd", "Ljava/io/FileDescriptor;");
    if (! FileChannelImpl_fd) {
        return 0;
    }
    FileDescriptor_fd = (*env)->GetFieldID(env, FileDescriptor, "fd", "I");
    if (! FileDescriptor_fd) {
        return 0;
    }
    jintArray array = (*env)->NewIntArray(env, 6);
    if (! array) {
        return NULL;
    }
    jint realInts[6];
    realInts[0] = deadFD();
    realInts[1] = EAGAIN;
    realInts[2] = EINTR;
    struct sockaddr_un unused;
    realInts[3] = sizeof unused.sun_path;
    realInts[4] = 0;
    if (kqueue) { realInts[4] |= 0x02; }
#ifdef DP_POLL
    realInts[4] |= 0x04;
#endif
#ifdef __linux__
    if (epoll_create) { realInts[4] |= 0x01; }
    if (splice) { realInts[4] |= 0x10; }
    if (sendfile) { realInts[4] |= 0x20; }
#endif
#ifdef TCP_CORK
    realInts[4] |= 0x40;
#endif
    realInts[5] = EBADF;

    (*env)->SetIntArrayRegion(env, array, 0, 6, realInts);
    return array;
}

static int deadFD() {
    int fds[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0) {
        return -errno;
    }
    if (shutdown(fds[0], SHUT_RDWR) < 0) {
        goto failed;
    }
    close(fds[1]); // result == don't care
    return fds[0];

failed: {
        int code = errno;
        close(fds[0]);
        close(fds[1]);
        return -code;
    }
}
