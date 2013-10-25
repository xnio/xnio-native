
#include "xnio.h"

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

extern int dup3(int, int, int) weak;

JNIEXPORT jint JNICALL xnio_native(dup2)(JNIEnv *env, jclass clazz, jint oldFD, jint newFD) {
    if (dup3) {
        if (dup3(oldFD, newFD, O_CLOEXEC) < 0) {
            return -errno;
        } else {
            return 0;
        }
    } else {
        if (dup2(oldFD, newFD) < 0) {
            return -errno;
        }
        if (fcntl(newFD, F_SETFD, FD_CLOEXEC) == -1) {
            int code = errno;
            close(newFD);
            return -code;
        }
        return 0;
    }
}
