
#include "xnio.h"

#include <unistd.h>
#include <sys/uio.h>
#include <errno.h>

JNIEXPORT jint JNICALL xnio_native(sendDirect)(JNIEnv *env, jclass clazz, jint fd, jobject bufferObj, jintArray posAndLimit, jbyteArray destAddrArray) {
    union sockaddr_any dest;
    jint dres;
    dres = decode(env, destAddrArray, &dest);
    if (dres < 0) {
        return dres;
    }
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
    if ((res = sendto(fd, buffer, limit - position, 0, &dest.addr, sizeof dest)) == -1) {
        return -errno;
    }
    if (res > 0) {
        position += res;
        (*env)->SetIntArrayRegion(env, posAndLimit, 0, 1, &position);
    }
    return res;
}

JNIEXPORT jlong JNICALL xnio_native(sendDirectGather)(JNIEnv *env, jclass clazz, jint fd, jobjectArray bufferObjs, jint offs, jint len, jintArray posArray, jintArray limitArray, jbyteArray destAddrArray) {
    struct iovec iov[len];
    ssize_t res;
    union sockaddr_any dest;
    jint dres;
    dres = decode(env, destAddrArray, &dest);
    if (dres < 0) {
        return dres;
    }

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

    struct msghdr msg = {
        .msg_name = &dest,
        .msg_namelen = sizeof dest,
        .msg_iov = iov,
        .msg_iovlen = len,
    };

    if ((res = sendmsg(fd, & msg, 0)) < 0) {
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

JNIEXPORT jint JNICALL xnio_native(sendHeap)(JNIEnv *env, jclass clazz, jint fd, jbyteArray bufferObj, jintArray posAndLimit, jbyteArray destAddrArray) {
    union sockaddr_any dest;
    jint dres;
    dres = decode(env, destAddrArray, &dest);
    if (dres < 0) {
        return dres;
    }
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
    if ((res = sendto(fd, buffer, limit - position, 0, &dest.addr, sizeof dest)) == -1) {
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

JNIEXPORT jlong JNICALL xnio_native(sendHeapGather)(JNIEnv *env, jclass clazz, jint fd, jobjectArray bufferObjs, jint offs, jint len, jintArray posArray, jintArray limitArray, jbyteArray destAddrArray) {
    struct iovec iov[len];
    ssize_t res;
    union sockaddr_any dest;
    jint dres;
    dres = decode(env, destAddrArray, &dest);
    if (dres < 0) {
        return dres;
    }

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

    struct msghdr msg = {
        .msg_name = &dest,
        .msg_namelen = sizeof dest,
        .msg_iov = iov,
        .msg_iovlen = len,
    };

    res = sendmsg(fd, & msg, 0);

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
