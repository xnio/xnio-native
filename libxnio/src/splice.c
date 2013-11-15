
#include "xnio.h"

#include <unistd.h>
#include <errno.h>
#include <fcntl.h>

#ifdef __linux__

extern ssize_t splice(int, loff_t *, int, loff_t *, size_t, unsigned int) weak;

JNIEXPORT jlong JNICALL xnio_native(spliceToFile)(JNIEnv *env, jclass clazz, jint srcFd, jobject fileChannel, jlong offset, jlong length) {
    ssize_t res;
    loff_t off = offset;
    jobject fileDescriptor = (*env)->GetObjectField(env, fileChannel, FileChannelImpl_fd);
    if (! fileDescriptor) {
        return -EINVAL;
    }
    jint destFd = (*env)->GetIntField(env, fileDescriptor, FileDescriptor_fd);
    if (destFd == -1) {
        return -EINVAL;
    }

    while ((res = splice(srcFd, NULL, destFd, &off, length, SPLICE_F_NONBLOCK)) == -1) {
        int err = errno;
        if (err != EINTR) {
            return -err;
        }
    }
    return res;
}

JNIEXPORT jlong JNICALL xnio_native(transfer)(JNIEnv *env, jclass clazz, jint srcFd, jlong count, jobject buffer, jint destFd) {

    ssize_t res;

    (*env)->SetIntField(env, buffer, Buffer_pos, 0);

    for (;;) {
        res = splice(srcFd, NULL, destFd, NULL, count, SPLICE_F_NONBLOCK | SPLICE_F_MOVE);
        if (res == -1) {
            int err = errno;
            if (err == EAGAIN) {
                // we would block for some reason; we're done, but try to grab some read data into the thru buffer
                void *addr = (*env)->GetDirectBufferAddress(env, buffer);
                if (addr) {
                    // direct buffer xfer
                    jlong size = (*env)->GetDirectBufferCapacity(env, buffer);
                    if (size == -1) {
                        // just forget it, man
                        (*env)->SetIntField(env, buffer, Buffer_lim, 0);
                        return -EAGAIN;
                    }
                    size_t res2;
                    while ((res2 = read(srcFd, addr, size)) == -1) {
                        err = errno;
                        if (err == EAGAIN) {
                            // block on read not write
                            (*env)->SetIntField(env, buffer, Buffer_lim, 0);
                            return -EAGAIN;
                        } else if (err != EINTR) {
                            return -err;
                        }
                    }
                    (*env)->SetIntField(env, buffer, Buffer_lim, res2);
                    return -EAGAIN;
                } else {
                    // heap buffer xfer
                    jint offset = (*env)->GetIntField(env, buffer, ByteBuffer_offset);
                    jint size = (*env)->GetIntField(env, buffer, Buffer_lim);
                    jbyteArray array = (*env)->GetObjectField(env, buffer, ByteBuffer_array);
                    if (! array) {
                        return -EINVAL;
                    }
                    jbyte *bytes = (*env)->GetByteArrayElements(env, array, 0);
                    if (! bytes) {
                        return -ENOMEM;
                    }
                    size_t res2;
                    while ((res2 = read(srcFd, addr + offset, size)) == -1) {
                        err = errno;
                        if (err == EAGAIN) {
                            // block on read not write
                            (*env)->SetIntField(env, buffer, Buffer_lim, 0);
                            (*env)->ReleaseByteArrayElements(env, array, bytes, JNI_ABORT);
                            return -EAGAIN;
                        } else if (err != EINTR) {
                            (*env)->ReleaseByteArrayElements(env, array, bytes, JNI_ABORT);
                            return -err;
                        }
                    }
                    (*env)->SetIntField(env, buffer, Buffer_lim, res2);
                    (*env)->ReleaseByteArrayElements(env, array, bytes, 0);
                    return -EAGAIN;
                }
                // not reachable
            } else if (err != EINTR) {
                // real problem here...
                // nothing to write
                (*env)->SetIntField(env, buffer, Buffer_lim, 0);
                return -err;
            }
            // fall through and repeat for EINTR
        } else {
            // transferred some stuff
            (*env)->SetIntField(env, buffer, Buffer_lim, 0);
            return res;
        }
        // loop back to top to retry EINTR
    }
}

#endif


