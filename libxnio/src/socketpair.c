
#include "xnio.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

JNIEXPORT jint JNICALL xnio_native(socketPair)(JNIEnv *env, jclass clazz, jintArray jfds, jobject preserve) {
    int fds[2];
#if defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
    if (socketpair(AF_LOCAL, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) == -1) {
#endif
        if (socketpair(AF_LOCAL, SOCK_STREAM, 0, fds) == -1) {
            return -errno;
        }
        if (fcntl(fds[0], F_SETFD, FD_CLOEXEC) == -1) {
            goto failed;
        }
        if (fcntl(fds[1], F_SETFD, FD_CLOEXEC) == -1) {
            goto failed;
        }
        if (fcntl(fds[0], F_SETFL, O_NONBLOCK) == -1) {
            goto failed;
        }
        if (fcntl(fds[1], F_SETFL, O_NONBLOCK) == -1) {
            goto failed;
        }
#if defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
    }
#endif
    jint *array = (*env)->GetPrimitiveArrayCritical(env, jfds, 0);
    if (! array) {
        close(fds[0]);
        close(fds[1]);
        return -ENOMEM;
    }
    array[0] = fds[0];
    array[1] = fds[1];
    (*env)->ReleasePrimitiveArrayCritical(env, jfds, array, 0);
    return 0;

failed: {
        int code = errno;
        close(fds[0]);
        close(fds[1]);
        return -code;
    }
}
