
#include "xnio.h"

#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>

extern int accept4(int, struct sockaddr *, socklen_t *, int) weak;

JNIEXPORT jint JNICALL xnio_native(accept)(JNIEnv *env, jclass clazz, jint fd, jobject preserve) {
    jint nfd;
    if (accept4) {
        if ((nfd = accept4(fd, NULL, 0, SOCK_NONBLOCK | SOCK_CLOEXEC)) == -1) {
            return -errno;
        } else {
            return nfd;
        }
    } else if ((nfd = accept(fd, NULL, 0)) == -1) {
        return -errno;
    } else {
        if (fcntl(fd, F_SETFD, FD_CLOEXEC) == -1) {
            goto failed;
        }
        if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1) {
            goto failed;
        }
        return nfd;
    }

failed: {
    int code = errno;
    close(nfd);
    return -code;
}
}
