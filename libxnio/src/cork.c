
#include "xnio.h"

#include <unistd.h>
#include <errno.h>
#include <fcntl.h>

#ifdef __linux__
#ifdef TCP_CORK

JNIEXPORT void JNICALL xnio_native(flushTcpCork)(JNIEnv *env, jclass clazz, jint fd) {
    int cork = 0;
    int res;
    for (cork = 0; cork < 2; cork ++) {
        while ((res = setsockopt(fd, IPPROTO_TCP, TCP_CORK, &cork, sizeof cork)) == -1) {
            int err = errno;
            if (err != EINTR) {
                return -err;
            }
        }
    }
}

#endif
#endif


