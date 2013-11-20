
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

JNIEXPORT jint JNICALL xnio_native(epollWait)(JNIEnv *env, jclass clazz, jint efd, jlongArray eventArray, jint timeout) {
    int count = (*env)->GetArrayLength(env, eventArray);
    struct epoll_event events[count];
    int res = epoll_wait(efd, events, count, (int) timeout);
    if (res < 0) {
        return -errno;
    }
    if (res == 0) {
        return 0;
    }
    jlong *items = (*env)->GetLongArrayElements(env, eventArray, 0);
    if (! items) {
        return -ENOMEM;
    }
    for (int i = 0; i < res; i ++) {
        items[i] = events[i].data.u64;
        if (events[i].events & (EPOLLIN | EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
            items[i] |= XNIO_EPOLL_READ;
        }
        if (events[i].events & (EPOLLOUT | EPOLLERR | EPOLLHUP)) {
            items[i] |= XNIO_EPOLL_WRITE;
        }
    }
    (*env)->ReleaseLongArrayElements(env, eventArray, items, 0);
    return res;
}

JNIEXPORT jint JNICALL xnio_native(epollCtlAdd)(JNIEnv *env, jclass clazz, jint efd, jint fd, jint flags, jint id) {
    uint32_t events = 0;
    if (flags & XNIO_EPOLL_READ) {
        events |= EPOLLIN | EPOLLRDHUP | EPOLLERR | EPOLLHUP;
    }
    if (flags & XNIO_EPOLL_WRITE) {
        events |= EPOLLOUT | EPOLLERR | EPOLLHUP;
    }
    if (flags & XNIO_EPOLL_EDGE) {
        events |= EPOLLET;
    }
    struct epoll_event event = {
        .events = events,
        .data.u64 = (((uint64_t) id) << 32L)
    };
    if (epoll_ctl(efd, EPOLL_CTL_ADD, fd, &event) < 0) {
        return -errno;
    }
    return 0;
}

JNIEXPORT jint JNICALL xnio_native(epollCtlMod)(JNIEnv *env, jclass clazz, jint efd, jint fd, jint flags, jint id) {
    uint32_t events = 0;
    if (flags & XNIO_EPOLL_READ) {
        events |= EPOLLIN | EPOLLRDHUP | EPOLLERR | EPOLLHUP;
    }
    if (flags & XNIO_EPOLL_WRITE) {
        events |= EPOLLOUT | EPOLLERR | EPOLLHUP;
    }
    if (flags & XNIO_EPOLL_EDGE) {
        events |= EPOLLET;
    }
    struct epoll_event event = {
        .events = events,
        .data.u64 = (((uint64_t) id) << 32L)
    };
    if (epoll_ctl(efd, EPOLL_CTL_MOD, fd, &event) < 0) {
        return -errno;
    }
    return 0;
}

JNIEXPORT jint JNICALL xnio_native(epollCtlDel)(JNIEnv *env, jclass clazz, jint efd, jint fd) {
    struct epoll_event event = { 0 };
    if (epoll_ctl(efd, EPOLL_CTL_DEL, fd, &event) < 0) {
        return -errno;
    }
    return 0;
}

#endif /* __linux__ */
