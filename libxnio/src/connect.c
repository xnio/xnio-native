
#include "xnio.h"

#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>

JNIEXPORT jint JNICALL xnio_native(connect)(JNIEnv *env, jclass clazz, jint fd, jbyteArray destArray) {
    union sockaddr_any addr;
    jint res;
    res = decode(env, destArray, &addr);
    if (res < 0) {
        return res;
    }
    switch (addr.addr.sa_family) {
        case AF_UNIX: {
            res = connect(fd, &addr.addr, sizeof(struct sockaddr_un));
            break;
        }
        default: {
            res = connect(fd, &addr.addr, sizeof addr);
            break;
        }
    }
    if (res < 0) {
        int e = errno;
        if (e == EAGAIN) {
            return -EBUSY;
        } else if (e == EINPROGRESS) {
            return -EAGAIN;
        } else {
            return -e;
        }
    }
    return res;
}

JNIEXPORT jint JNICALL xnio_native(finishConnect)(JNIEnv *env, jclass clazz, jint fd) {
    int res = 0;
    socklen_t len = sizeof res;
    int r2 = getsockopt(fd, SOL_SOCKET, SO_ERROR, &res, &len);
    if (r2 < 0) {
        return -errno;
    }
    if (res == EAGAIN) {
        return -EBUSY;
    } else if (res == EINPROGRESS) {
        return -EAGAIN;
    } else {
        return -res;
    }
}