
#include "xnio.h"

#include <unistd.h>
#include <sys/uio.h>
#include <errno.h>

JNIEXPORT jint JNICALL xnio_native(writeLong)(JNIEnv *env, jclass clazz, jint fd, jlong value, jobject preserve) {
    ssize_t res;
    while ((res = write(fd, &value, sizeof value)) == -1) {
        int err = errno;
        if (err != EINTR) {
            return -err;
        }
    }
    return 0;
}

JNIEXPORT jint JNICALL xnio_native(writeD)(JNIEnv *env, jclass clazz, jint fd, jobject b1, jint p1, jint l1, jobject preserve) {
    void *buffer = (*env)->GetDirectBufferAddress(env, b1);
    if (! buffer) {
        return -EINVAL;
    }
    ssize_t res;
    while ((res = write(fd, buffer + p1, l1 - p1)) == -1) {
        int err = errno;
        if (err != EINTR) {
            return -err;
        }
    }
    return res;
}

JNIEXPORT jlong JNICALL xnio_native(writeDD)(JNIEnv *env, jclass clazz, jint fd, jobject b1, jint p1, jint l1, jobject b2, jint p2, jint l2, jobject preserve) {
    struct iovec iov[2];
    void *buffer1 = (*env)->GetDirectBufferAddress(env, b1);
    if (! buffer1) {
        return -EINVAL;
    }
    void *buffer2 = (*env)->GetDirectBufferAddress(env, b2);
    if (! buffer2) {
        return -EINVAL;
    }
    iov[0].iov_base = buffer1 + p1;
    iov[0].iov_len = l1 - p1;
    iov[1].iov_base = buffer2 + p2;
    iov[1].iov_len = l2 - p2;
    ssize_t res;
    while ((res = writev(fd, iov, 2)) == -1) {
        int err = errno;
        if (err != EINTR) {
            return -err;
        }
    }
    return res;
}

JNIEXPORT jlong JNICALL xnio_native(writeDDD)(JNIEnv *env, jclass clazz, jint fd, jobject b1, jint p1, jint l1, jobject b2, jint p2, jint l2, jobject b3, jint p3, jint l3, jobject preserve) {
    struct iovec iov[3];
    void *buffer1 = (*env)->GetDirectBufferAddress(env, b1);
    if (! buffer1) {
        return -EINVAL;
    }
    void *buffer2 = (*env)->GetDirectBufferAddress(env, b2);
    if (! buffer2) {
        return -EINVAL;
    }
    void *buffer3 = (*env)->GetDirectBufferAddress(env, b3);
    if (! buffer3) {
        return -EINVAL;
    }
    iov[0].iov_base = buffer1 + p1;
    iov[0].iov_len = l1 - p1;
    iov[1].iov_base = buffer2 + p2;
    iov[1].iov_len = l2 - p2;
    iov[2].iov_base = buffer3 + p3;
    iov[2].iov_len = l3 - p3;
    ssize_t res;
    while ((res = writev(fd, iov, 3)) == -1) {
        int err = errno;
        if (err != EINTR) {
            return -err;
        }
    }
    return res;
}

JNIEXPORT jint JNICALL xnio_native(writeH)(JNIEnv *env, jclass clazz, jint fd, jbyteArray b1, jint p1, jint l1, jobject preserve) {
    jbyte *buffer = (*env)->GetByteArrayElements(env, b1, 0);
    if (! buffer) {
        return -ENOMEM;
    }
    ssize_t res;
    while ((res = write(fd, buffer + p1, l1 - p1)) == -1) {
        int err = errno;
        if (err != EINTR) {
            (*env)->ReleaseByteArrayElements(env, b1, buffer, JNI_ABORT);
            return -err;
        }
    }
    (*env)->ReleaseByteArrayElements(env, b1, buffer, JNI_ABORT);
    return res;
}

JNIEXPORT jint JNICALL xnio_native(writeHH)(JNIEnv *env, jclass clazz, jint fd, jbyteArray b1, jint p1, jint l1, jbyteArray b2, jint p2, jint l2, jobject preserve) {
    struct iovec iov[2];
    jbyte *buffer1 = (*env)->GetByteArrayElements(env, b1, 0);
    if (! buffer1) {
        return -ENOMEM;
    }
    jbyte *buffer2 = (*env)->GetByteArrayElements(env, b2, 0);
    if (! buffer2) {
        (*env)->ReleaseByteArrayElements(env, b1, buffer1, JNI_ABORT);
        return -ENOMEM;
    }
    iov[0].iov_base = buffer1;
    iov[0].iov_len = l1 - p1;
    iov[1].iov_base = buffer2;
    iov[1].iov_len = l2 - p2;
    ssize_t res;
    while ((res = writev(fd, iov, 2)) == -1) {
        int err = errno;
        if (err != EINTR) {
            (*env)->ReleaseByteArrayElements(env, b2, buffer2, JNI_ABORT);
            (*env)->ReleaseByteArrayElements(env, b1, buffer1, JNI_ABORT);
            return -err;
        }
    }
    (*env)->ReleaseByteArrayElements(env, b2, buffer2, JNI_ABORT);
    (*env)->ReleaseByteArrayElements(env, b1, buffer1, JNI_ABORT);
    return res;
}

JNIEXPORT jint JNICALL xnio_native(writeHHH)(JNIEnv *env, jclass clazz, jint fd, jbyteArray b1, jint p1, jint l1, jbyteArray b2, jint p2, jint l2, jbyteArray b3, jint p3, jint l3, jobject preserve) {
    struct iovec iov[3];
    jbyte *buffer1 = (*env)->GetByteArrayElements(env, b1, 0);
    if (! buffer1) {
        return -ENOMEM;
    }
    jbyte *buffer2 = (*env)->GetByteArrayElements(env, b2, 0);
    if (! buffer2) {
        (*env)->ReleaseByteArrayElements(env, b1, buffer1, JNI_ABORT);
        return -ENOMEM;
    }
    jbyte *buffer3 = (*env)->GetByteArrayElements(env, b3, 0);
    if (! buffer3) {
        (*env)->ReleaseByteArrayElements(env, b2, buffer2, JNI_ABORT);
        (*env)->ReleaseByteArrayElements(env, b1, buffer1, JNI_ABORT);
        return -ENOMEM;
    }
    iov[0].iov_base = buffer1;
    iov[0].iov_len = l1 - p1;
    iov[1].iov_base = buffer2;
    iov[1].iov_len = l2 - p2;
    iov[2].iov_base = buffer3;
    iov[2].iov_len = l3 - p3;
    ssize_t res;
    while ((res = writev(fd, iov, 3)) == -1) {
        int err = errno;
        if (err != EINTR) {
            (*env)->ReleaseByteArrayElements(env, b3, buffer3, JNI_ABORT);
            (*env)->ReleaseByteArrayElements(env, b2, buffer2, JNI_ABORT);
            (*env)->ReleaseByteArrayElements(env, b1, buffer1, JNI_ABORT);
            return -err;
        }
    }
    (*env)->ReleaseByteArrayElements(env, b3, buffer3, JNI_ABORT);
    (*env)->ReleaseByteArrayElements(env, b2, buffer2, JNI_ABORT);
    (*env)->ReleaseByteArrayElements(env, b1, buffer1, JNI_ABORT);
    return res;
}

static jlong writeMisc_internal(JNIEnv *env, jclass clazz, jint fd, jobjectArray buffers, jint offs, jint len, jint idx, struct iovec *iov) {
    if (offs + idx == len) {
        ssize_t res;
        while ((res = writev(fd, iov, idx)) == -1) {
            int err = errno;
            if (err != EINTR) {
                return -err;
            }
        }
        return res;
    }

    jobject bufObj = (*env)->GetObjectArrayElement(env, buffers, offs + idx);
    if (! bufObj) {
        iov[idx].iov_base = 0;
        iov[idx].iov_len = 0;
        return writeMisc_internal(env, clazz, fd, buffers, offs, len, idx + 1, iov);
    } else {
        jint pos = (*env)->GetIntField(env, bufObj, Buffer_pos);
        jint lim = (*env)->GetIntField(env, bufObj, Buffer_lim);

        void *buffer = (*env)->GetDirectBufferAddress(env, bufObj);
        if (buffer) {
            iov[idx].iov_base = buffer + pos;
            iov[idx].iov_len = lim - pos;
            return writeMisc_internal(env, clazz, fd, buffers, offs, len, idx + 1, iov);
        } else {
            // heap or nothing
            jint offset = (*env)->GetIntField(env, bufObj, ByteBuffer_offset);
            jbyteArray array = (*env)->GetObjectField(env, bufObj, ByteBuffer_array);
            if (! array) {
                return -EINVAL;
            }
            jbyte *bytes = (*env)->GetByteArrayElements(env, array, 0);
            if (! bytes) {
                return -ENOMEM;
            }
            iov[idx].iov_base = bytes + offset + pos;
            iov[idx].iov_len = lim - pos;
            jlong res = writeMisc_internal(env, clazz, fd, buffers, offs, len, idx + 1, iov);
            (*env)->ReleaseByteArrayElements(env, array, bytes, JNI_ABORT);
            return res;
        }
        // not reachable
    }
    // not reachable
}

JNIEXPORT jlong JNICALL xnio_native(writeMisc)(JNIEnv *env, jclass clazz, jint fd, jobjectArray buffers, jint offs, jint len, jobject preserve) {
    struct iovec iov[len];
    return writeMisc_internal(env, clazz, fd, buffers, offs, len, 0, iov);
}
