
#include "xnio.h"

#include <poll.h>
#include <errno.h>
#include <signal.h>

extern int ppoll(struct pollfd *, nfds_t, const struct timespec *, const sigset_t *) weak;

JNIEXPORT jint JNICALL xnio_native(await2)(JNIEnv *env, jclass clazz, jint fd, jboolean writes) {
    struct pollfd fds = { .fd = fd, .events = writes ? POLLOUT | POLLERR | POLLHUP | POLLNVAL : POLLIN | POLLRDHUP };
//    if (ppoll) {
//        struct timespec ts = { tv_sec = -1, tv_nsec = -1 };
//        sigset_t sm;
//
//        if (ppoll(&fds, 1, &ts, &sm) == -1) {
//            return -errno;
//        }
//    } else {
        if (poll(&fds, 1, -1) < 0) {
            return -errno;
        }
//    }
    return 0;
}

JNIEXPORT jint JNICALL xnio_native(await3)(JNIEnv *env, jclass clazz, jint fd, jboolean writes, jlong millis) {
    struct pollfd fds = { .fd = fd, .events = writes ? POLLOUT | POLLERR | POLLHUP | POLLNVAL : POLLIN | POLLRDHUP };
//    if (ppoll) {
//        struct timespec ts = { tv_sec = -1, tv_nsec = -1 };
//        sigset_t sm;
//
//        if (ppoll(&fds, 1, &ts, &sm) == -1) {
//            return -errno;
//        }
//    } else {
        if (poll(&fds, 1, millis > 0x7fffffff ? 0x7fffffff : (int) millis) < 0) {
            return -errno;
        }
//    }
    return 0;
}
