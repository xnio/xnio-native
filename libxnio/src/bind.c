
#include "xnio.h"

#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>

JNIEXPORT jint JNICALL xnio_native(bind)(JNIEnv *env, jclass clazz, jint fd, jbyteArray bindArray) {
    union sockaddr_any addr;
    jint res;
    res = decode(env, bindArray, &addr);
    if (res < 0) {
        return res;
    }
    switch (addr.addr.sa_family) {
        case AF_UNIX: {
            res = bind(fd, &addr.addr, sizeof(struct sockaddr_un));
            break;
        }
        default: {
            res = bind(fd, &addr.addr, sizeof addr);
            break;
        }
    }
    if (res < 0) {
        res = -errno;
    }
    return res;
}
