
#include "xnio.h"

#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#ifdef __linux__
#ifdef TCP_CORK

JNIEXPORT jint JNICALL xnio_native(flushTcpCork)(JNIEnv *env, jclass clazz, jint fd, jobject preserve) {
    int cork = 0;
    int res;
    if ((res = setsockopt(fd, IPPROTO_TCP, TCP_CORK, &cork, sizeof cork)) == -1) {
        int err = errno;
        if (err != EINTR) {
            return -err;
        }
    }
    cork = 1;
    if ((res = setsockopt(fd, IPPROTO_TCP, TCP_CORK, &cork, sizeof cork)) == -1) {
        int err = errno;
        if (err != EINTR) {
            return -err;
        }
    }
    return 0;
}

#endif
#endif


