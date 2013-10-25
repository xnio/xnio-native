
#include "xnio.h"

#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/tcp.h>

static int deadFD();

extern int epoll_create(int) weak;
extern int kqueue(void) weak;
extern ssize_t splice(int, loff_t *, int, loff_t *, size_t, unsigned int) weak;
#ifdef __LINUX__
extern ssize_t sendfile(int, int, off_t *, size_t) weak;
#else
extern int sendfile(int, int, off_t, off_t *, void *, int) weak;
#endif

JNIEXPORT jintArray JNICALL xnio_native(init)(JNIEnv *env, jclass clazz) {
    jintArray array = (*env)->NewIntArray(env, 5);
    if (! array) {
        return NULL;
    }
    jint realInts[5];
    realInts[0] = deadFD();
    realInts[1] = EAGAIN;
    realInts[2] = EINTR;
    struct sockaddr_un unused;
    realInts[3] = sizeof unused.sun_path;
    realInts[4] = 0;
    if (epoll_create) { realInts[4] |= 0x01; }
    if (kqueue) { realInts[4] |= 0x02; }
#ifdef DP_POLL
    realInts[4] |= 0x04;
#endif
    if (splice) { realInts[4] |= 0x10; }
    if (sendfile) { realInts[4] |= 0x20; }
#ifdef TCP_CORK
    realInts[4] |= 0x40;
#endif

    (*env)->SetIntArrayRegion(env, array, 0, 5, realInts);
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
