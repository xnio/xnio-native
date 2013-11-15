
#include "xnio.h"

#include <unistd.h>
#include <errno.h>

#ifdef __linux__

extern ssize_t sendfile(int, int, off_t *, size_t) weak;

JNIEXPORT jint JNICALL xnio_native(sendfile)(JNIEnv *env, jclass clazz, jint destFd, jobject fileChannel, jlong offset, jlong length) {
    ssize_t res;
    off_t off = offset;
    jobject fileDescriptor = (*env)->GetObjectField(env, fileChannel, FileChannelImpl_fd);
    if (! fileDescriptor) {
        return -EINVAL;
    }
    jint srcFd = (*env)->GetIntField(env, fileDescriptor, FileDescriptor_fd);
    if (srcFd == -1) {
        return -EINVAL;
    }
    while ((res = sendfile(destFd, srcFd, &off, length)) == -1) {
        int err = errno;
        if (err != EINTR) {
            return -err;
        }
    }
    return res;
}

#endif


