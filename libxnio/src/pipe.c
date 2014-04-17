
#include "xnio.h"

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

extern int pipe2(int[2], int) weak;

JNIEXPORT jint JNICALL xnio_native(pipe)(JNIEnv *env, jclass clazz, jintArray jfds, jobject preserve) {
    int fds[2];
    if (pipe2) {
        if (pipe2(fds, O_CLOEXEC | O_NONBLOCK) == -1) {
            return -errno;
        }
    } else {
        if (pipe(fds) == -1) {
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
    }
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
