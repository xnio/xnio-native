
#include "xnio.h"

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

JNIEXPORT jint JNICALL xnio_native(dup)(JNIEnv *env, jclass clazz, jint oldFD) {
    jint newFD;
    if ((newFD = dup(oldFD)) < 0) {
        return -errno;
    }
    if (fcntl(newFD, F_SETFD, FD_CLOEXEC) == -1) {
        int code = errno;
        close(newFD);
        return -code;
    }
    return newFD;
}
