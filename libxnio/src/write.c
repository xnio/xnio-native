
#include "xnio.h"

#include <unistd.h>
#include <sys/uio.h>
#include <errno.h>

JNIEXPORT jint JNICALL xnio_native(writeLong)(JNIEnv *env, jclass clazz, jint fd, jlong value) {
    ssize_t res = write(fd, &value, sizeof value);
    if (res < 0) {
        return -errno;
    }
    return 0;
}

JNIEXPORT jint JNICALL xnio_native(writeDirect)(JNIEnv *env, jclass clazz, jint fd, jobject bufferObj, jintArray posAndLimit) {
    jint position, limit;
    void *buffer = (*env)->GetDirectBufferAddress(env, bufferObj);
    if (! buffer) {
        return -EINVAL;
    }
    jint *pal = (*env)->GetPrimitiveArrayCritical(env, posAndLimit, 0);
    if (! pal) {
        return -ENOMEM;
    }
    position = pal[0];
    limit = pal[1];
    (*env)->ReleasePrimitiveArrayCritical(env, posAndLimit, pal, JNI_ABORT);
    ssize_t res;
    if ((res = write(fd, buffer, limit - position)) == -1) {
        return -errno;
    }
    if (res > 0) {
        position += res;
        (*env)->SetIntArrayRegion(env, posAndLimit, 0, 1, &position);
    }
    return res;
}

JNIEXPORT jlong JNICALL xnio_native(writeDirectGather)(JNIEnv *env, jclass clazz, jint fd, jobjectArray bufferObjs, jint offs, jint len, jintArray posArray, jintArray limitArray) {
    struct iovec iov[len];
    ssize_t res;

    jint *pos = (*env)->GetPrimitiveArrayCritical(env, posArray, 0);
    if (! pos) {
        res = -ENOMEM;
        goto fail0;
    }
    jint *limit = (*env)->GetPrimitiveArrayCritical(env, limitArray, 0);
    if (! limit) {
        res = -ENOMEM;
        goto fail1;
    }
    for (int i = 0; i < len; i ++) {
        if (! (iov[i].iov_base = (*env)->GetDirectBufferAddress(env, (*env)->GetObjectArrayElement(env, bufferObjs, i)))) {
            res = -EINVAL;
            goto fail2;
        }
        iov[i].iov_len = limit[i] - pos[i];
    }
    (*env)->ReleasePrimitiveArrayCritical(env, limitArray, limit, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, posArray, pos, JNI_ABORT);

    if ((res = writev(fd, iov, len)) < 0) {
        return -errno;
    }

    jlong ret = res;
    jint n;
    for (int i = 0; i < len; i++) {
        size_t s = iov[i].iov_len;
        if (s > res) {
            n = res;
            (*env)->SetIntArrayRegion(env, posArray, i, 1, &n);
            break;
        } else {
            jint n = s;
            (*env)->SetIntArrayRegion(env, posArray, i, 1, &n);
            res -= s;
        }
    }
    return ret;

fail2:
    (*env)->ReleasePrimitiveArrayCritical(env, limitArray, limit, JNI_ABORT);
fail1:
    (*env)->ReleasePrimitiveArrayCritical(env, posArray, pos, JNI_ABORT);
fail0:
    return res;
}

JNIEXPORT jint JNICALL xnio_native(writeHeap)(JNIEnv *env, jclass clazz, jint fd, jbyteArray bufferObj, jintArray posAndLimit) {
    jint position, limit;
    jbyte *buffer = (*env)->GetByteArrayElements(env, bufferObj, 0);
    if (! buffer) {
        return -ENOMEM;
    }
    jint *pal = (*env)->GetPrimitiveArrayCritical(env, posAndLimit, 0);
    if (! pal) {
        (*env)->ReleaseByteArrayElements(env, bufferObj, buffer, JNI_ABORT);
        return -ENOMEM;
    }
    position = pal[0];
    limit = pal[1];
    (*env)->ReleasePrimitiveArrayCritical(env, posAndLimit, pal, JNI_ABORT);
    ssize_t res;
    if ((res = write(fd, buffer, limit - position)) < 0) {
        (*env)->ReleaseByteArrayElements(env, bufferObj, buffer, JNI_ABORT);
        return -errno;
    }
    (*env)->ReleaseByteArrayElements(env, bufferObj, buffer, JNI_ABORT);
    if (res > 0) {
        position += res;
        (*env)->SetIntArrayRegion(env, posAndLimit, 0, 1, &position);
    }
    return res;
}

JNIEXPORT jlong JNICALL xnio_native(writeHeapGather)(JNIEnv *env, jclass clazz, jint fd, jobjectArray bufferObjs, jint offs, jint len, jintArray posArray, jintArray limitArray) {
    struct iovec iov[len];
    ssize_t res;

    jint *pos = (*env)->GetPrimitiveArrayCritical(env, posArray, 0);
    if (! pos) {
        res = -ENOMEM;
        goto fail0;
    }
    jint *limit = (*env)->GetPrimitiveArrayCritical(env, limitArray, 0);
    if (! limit) {
        res = -ENOMEM;
        goto fail1;
    }
    for (int i = 0; i < len; i ++) {
        jbyteArray array = (jbyteArray) (*env)->GetObjectArrayElement(env, bufferObjs, i);
        if (array) {
            jbyte *bytes = (*env)->GetByteArrayElements(env, array, 0);
            if (bytes) {
                iov[i].iov_base = bytes;
                iov[i].iov_len = limit[i] - pos[i];
                continue;
            }
        }
        for (i--; i >= 0; i--) {
            (*env)->ReleaseByteArrayElements(env, array, (jbyte *) iov[i].iov_base, JNI_ABORT);
        }
        res = -ENOMEM;
        goto fail2;
    }
    (*env)->ReleasePrimitiveArrayCritical(env, limitArray, limit, JNI_ABORT);
    (*env)->ReleasePrimitiveArrayCritical(env, posArray, pos, JNI_ABORT);

    res = writev(fd, iov, len);

    jlong ret = res < 0 ? -errno : res;
    jint n;
    for (int i = 0; i < len; i++) {
        jbyteArray array = (jbyteArray) (*env)->GetObjectArrayElement(env, bufferObjs, i);
        (*env)->ReleaseByteArrayElements(env, array, (jbyte *) iov[i].iov_base, JNI_ABORT);
        if (res >= 0) {
            size_t s = iov[i].iov_len;
            n = s > res ? res : s;
            res -= s;
            (*env)->SetIntArrayRegion(env, posArray, i, 1, &n);
        }
    }
    return ret;

fail2:
    (*env)->ReleasePrimitiveArrayCritical(env, limitArray, limit, JNI_ABORT);
fail1:
    (*env)->ReleasePrimitiveArrayCritical(env, posArray, pos, JNI_ABORT);
fail0:
    return res;
}
