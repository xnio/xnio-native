
#include "xnio.h"

#include <sys/eventfd.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

extern int eventfd(int, int) weak;

JNIEXPORT jint JNICALL xnio_native(eventFD)(JNIEnv *env, jclass clazz) {
    int fd;
    if (eventfd) {
#if defined(EFD_CLOEXEC) && defined(EFD_NONBLOCK)
        fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (fd < 0) {
#define CB1
#endif
            fd = eventfd(0, 0);
            if (fd < 0) {
                return -errno;
            }
            if (fcntl(fd, F_SETFD, FD_CLOEXEC) == -1) {
                goto failed;
            }
            if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1) {
                goto failed;
            }
#ifdef CB1
        }
#endif
    } else {
        return -EINVAL;
    }
    return fd;

failed: {
        int code = errno;
        close(fd);
        return -code;
    }
}
