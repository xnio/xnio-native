
#include "xnio.h"

#include <sys/socket.h>
#include <errno.h>

JNIEXPORT jint JNICALL xnio_native(shutdown)(JNIEnv *env, jclass clazz, jint fd, jboolean rd, jboolean wr, jobject preserve) {
    if (! rd && ! wr) {
        return -EINVAL;
    }
    if (shutdown(fd, rd ? (wr ? SHUT_RDWR : SHUT_RD) : SHUT_WR) == -1) {
        int res = errno;
        if (res == ENOTCONN) {
            return 0;
        }
        return -errno;
    }
    return 0;
}
