
#include "xnio.h"

#ifdef __linux__

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include <sys/epoll.h>

extern int epoll_create(int) weak;
extern int epoll_create1(int) weak;
extern int epoll_wait(int, struct epoll_event *, int, int) weak;
extern int epoll_pwait(int, struct epoll_event *, int, int, const sigset_t *) weak;
extern int epoll_ctl(int, int, int, struct epoll_event *) weak;

JNIEXPORT jint JNICALL xnio_native(epollCreate)(JNIEnv *env, jclass clazz) {
    jint fd;
    if (epoll_create1) {
        if ((fd = epoll_create1(EPOLL_CLOEXEC)) < 0) {
            fd = epoll_create1(0);
            if (fd < 0) {
                return -errno;
            }
            if (fcntl(fd, F_SETFD, FD_CLOEXEC) < 0) {
                int code = errno;
                close(fd);
                return -code;
            }
        }
    } else if (epoll_create) {
        // the size parameter is generally ignored
        if ((fd = epoll_create(64)) < 0) {
            return -errno;
        }
        if (fcntl(fd, F_SETFD, FD_CLOEXEC) < 0) {
            int code = errno;
            close(fd);
            return -code;
        }
    } else {
        return -EINVAL;
    }
    return fd;
}

JNIEXPORT jint JNICALL xnio_native(epollWait)(JNIEnv *env, jclass clazz, jint efd, jintArray fds, jint timeout) {
    int count = (*env)->GetArrayLength(env, fds);
    struct epoll_event events[count];
    if (epoll_wait) {
        int res = epoll_wait(efd, events, count, (int) timeout);
        if (res < 0) {
            return -errno;
        }
        jint *fdsItems = (*env)->GetIntArrayElements(env, fds, 0);
        if (! fdsItems) {
            return -ENOMEM;
        }
        for (int i = 0; i < res; i ++) {
            fdsItems[i] = events[i].data.fd;
        }
        (*env)->ReleaseIntArrayElements(env, fds, fdsItems, JNI_COMMIT);
        return res;
    } else {
        return -EINVAL;
    }
}

static jint do_epoll_ctl(jint efd, jint fd, int op, uint32_t events) {
    if (epoll_ctl) {
        struct epoll_event event = { .events = events, .data.fd = fd };
        if (epoll_ctl(efd, op, fd, &event) < 0) {
            return -errno;
        }
        return 0;
    } else {
        return -EINVAL;
    }
}

JNIEXPORT jint JNICALL xnio_native(epollCtlAdd)(JNIEnv *env, jclass clazz, jint efd, jint fd, jboolean read, jboolean write, jboolean edge) {
    return do_epoll_ctl(efd, fd, EPOLL_CTL_ADD, (read ? EPOLLIN : 0) | (write ? EPOLLOUT | EPOLLRDHUP : 0) | (edge ? EPOLLET : 0));
}

JNIEXPORT jint JNICALL xnio_native(epollCtlMod)(JNIEnv *env, jclass clazz, jint efd, jint fd, jboolean read, jboolean write, jboolean edge) {
    return do_epoll_ctl(efd, fd, EPOLL_CTL_MOD, (read ? EPOLLIN : 0) | (write ? EPOLLOUT | EPOLLRDHUP : 0) | (edge ? EPOLLET : 0));
}

JNIEXPORT jint JNICALL xnio_native(epollCtlDel)(JNIEnv *env, jclass clazz, jint efd, jint fd) {
    return do_epoll_ctl(efd, fd, EPOLL_CTL_DEL, 0);
}

#endif /* __linux__ */
