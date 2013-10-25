
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
    res = bind(fd, &addr.addr, sizeof addr);
    if (res < 0) {
        res = -errno;
    }
    return res;
}
