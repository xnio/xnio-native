
#include "xnio.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/ip.h>

static jint create_socket(int domain, int type) {
    int fd;
#if defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
#define CB1
    if ((fd = socket(domain, type | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)) == -1) {
#endif
        if ((fd = socket(domain, type, 0)) == -1) {
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
    return fd;

failed: {
        int code = errno;
        close(fd);
        return -code;
    }
}

JNIEXPORT jint JNICALL xnio_native(socketTcp)(JNIEnv *env, jclass clazz) {
    return create_socket(AF_INET, SOCK_STREAM);
}

JNIEXPORT jint JNICALL xnio_native(socketUdp)(JNIEnv *env, jclass clazz) {
    jint fd = create_socket(AF_INET, SOCK_DGRAM);
    if (fd >= 0) {
        const int on = 1;
#ifdef IP_RECVDSTADDR
        if (setsockopt(fd, IPPROTO_IP, IP_RECVDSTADDR, &on, sizeof(on)) < 0) {
#endif
#ifdef IP_RECVORIGDSTADDR
            if (setsockopt(fd, IPPROTO_IP, IP_RECVORIGDSTADDR, &on, sizeof(on)) < 0) {
#endif
#ifdef IP_PKTINFO
                if (setsockopt(fd, IPPROTO_IP, IP_PKTINFO, &on, sizeof(on)) < 0) {
#endif
                    // give up
#ifdef IP_PKTINFO
                }
#endif
#ifdef IP_RECVORIGDSTADDR
            }
#endif
#ifdef IP_RECVDSTADDR
        }
#endif
    }
    return fd;
}

JNIEXPORT jint JNICALL xnio_native(socketTcp6)(JNIEnv *env, jclass clazz) {
    return create_socket(AF_INET6, SOCK_STREAM);
}

JNIEXPORT jint JNICALL xnio_native(socketUdp6)(JNIEnv *env, jclass clazz) {
    jint fd = create_socket(AF_INET6, SOCK_DGRAM);
    if (fd >= 0) {
        const int on = 1;
#ifdef IPV6_PKTINFO
        if (setsockopt(fd, IPPROTO_IP, IPV6_PKTINFO, &on, sizeof(on)) < 0) {
#endif
            // give up
#ifdef IPV6_PKTINFO
        }
#endif
    }
    return fd;
}

JNIEXPORT jint JNICALL xnio_native(socketLocalStream)(JNIEnv *env, jclass clazz) {
    return create_socket(AF_LOCAL, SOCK_STREAM);
}

JNIEXPORT jint JNICALL xnio_native(socketLocalDatagram)(JNIEnv *env, jclass clazz) {
    return create_socket(AF_LOCAL, SOCK_DGRAM);
}

