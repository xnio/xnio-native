
#include "xnio.h"

#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

/*
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

    static native int getOptSendBuffer(int fd);

    static native int setOptSendBuffer(int fd, int size);

    static native int getOptDeferAccept(int fd);

    static native int setOptDeferAccept(int fd, boolean enabled);

    static native int getOptMaxSegSize(int fd);

    static native int setOptMaxSegSize(int fd, int size);

    static native int getOptMulticastTtl(int fd);

    static native int setOptMulticastTtl(int fd, boolean enabled);
*/

JNIEXPORT jint JNICALL xnio_native(getOptReuseAddr)(JNIEnv *env, jclass clazz, jint fd) {
    int res;
    socklen_t len = sizeof res;
    int r2 = getsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &res, &len);
    if (r2 < 0) {
        return -errno;
    }
    return res;
}

JNIEXPORT jint JNICALL xnio_native(setOptReuseAddr)(JNIEnv *env, jclass clazz, jint fd, jboolean value) {
    int v = (int) value;
    socklen_t len = sizeof v;
    int r2 = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &v, len);
    if (r2 < 0) {
        return -errno;
    }
    return 0;
}

JNIEXPORT jint JNICALL xnio_native(getOptTcpNoDelay)(JNIEnv *env, jclass clazz, jint fd) {
    int res;
    socklen_t len = sizeof res;
    int r2 = getsockopt(fd, IPPROTO_IP, TCP_NODELAY, &res, &len);
    if (r2 < 0) {
        return -errno;
    }
    return res;
}

JNIEXPORT jint JNICALL xnio_native(setOptTcpNoDelay)(JNIEnv *env, jclass clazz, jint fd, jboolean value) {
    int v = (int) value;
    socklen_t len = sizeof v;
    int r2 = setsockopt(fd, IPPROTO_IP, TCP_NODELAY, &v, len);
    if (r2 < 0) {
        return -errno;
    }
    return 0;
}
