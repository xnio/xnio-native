
#include "xnio.h"

#include <unistd.h>
#include <errno.h>

#ifdef __linux__

#include <sys/timerfd.h>

extern int timerfd_create(int, int) weak;
extern int timerfd_settime(int, int, const struct itimerspec *, struct itimerspec *) weak;

JNIEXPORT jint JNICALL xnio_native(createTimer)(JNIEnv *env, jclass clazz, jint seconds, jint nanos) {
    int fd;
    int res;
    struct itimerspec ts = {
        .it_value = {
            .tv_sec = seconds,
            .tv_nsec = nanos
        }
    };
    while ((fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC)) == -1) {
        int err = errno;
        if (err != EINTR) {
            return -err;
        }
    }
    while ((res = timerfd_settime(fd, 0, &ts, 0)) == -1) {
        int err = errno;
        if (err != EINTR) {
            close(fd);
            return -err;
        }
    }
    return fd;
}

#endif


