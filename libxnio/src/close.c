
#include "xnio.h"

#include <unistd.h>
#include <errno.h>

JNIEXPORT jint JNICALL xnio_native(close)(JNIEnv *env, jclass clazz, jint fd) {
    if (close(fd) == -1) {
        return -errno;
    }
    return 0;
}
