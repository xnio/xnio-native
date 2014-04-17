
#include "xnio.h"

#include <unistd.h>
#include <errno.h>

#ifdef __linux__

#include <sys/timerfd.h>

extern int timerfd_create(int, int) weak;
extern int timerfd_settime(int, int, const struct itimerspec *, struct itimerspec *) weak;

JNIEXPORT jint JNICALL xnio_native(createTimer)(JNIEnv *env, jclass clazz, jint seconds, jint nanos, jobject preserve) {
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

JNIEXPORT jint JNICALL xnio_native(readTimer)(JNIEnv *env, jclass clazz, jint fd, jobject preserve) {
    uint64_t val;
    ssize_t res;
    while ((res = read(fd, &val, sizeof val)) == -1) {
        int err = errno;
        if (err != EINTR && err != EAGAIN) {
            return -err;
        }
    }
    return 0;
}

#endif


