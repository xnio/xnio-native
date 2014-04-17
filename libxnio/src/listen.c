
#include "xnio.h"

#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>

JNIEXPORT jint JNICALL xnio_native(listen)(JNIEnv *env, jclass clazz, jint fd, jint backlog, jobject preserve) {
    if (listen(fd, backlog) == -1) {
        return -errno;
    }
    return 0;
}
