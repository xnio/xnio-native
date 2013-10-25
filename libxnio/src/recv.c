
#include "xnio.h"

#include <unistd.h>
#include <sys/uio.h>
#include <sys/socket.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/ip.h>

JNIEXPORT jint JNICALL xnio_native(recvDirect)(JNIEnv *env, jclass clazz, jint fd, jobject bufferObj, jintArray posAndLimit, jbyteArray srcAddrArray, jbyteArray destAddrArray) {
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
    union sockaddr_any src = {};
    union sockaddr_any dest = {};
    struct iovec iov = {
        .iov_base = buffer,
        .iov_len = limit - position,
    };
    union {
#ifdef IP_PKTINFO
        struct in_pktinfo pi;
#endif
#if defined(IP_RECVDSTADDR) || defined(IP_ORIGDSTADDR)
        struct sockaddr_in sa;
#endif
#if defined(IPV6_PKTINFO)
        struct in6_pktinfo pi6;
#endif
        char unused[48];
    } hdr;
    struct msghdr msg = {
        .msg_name = &src,
        .msg_namelen = sizeof src,
        .msg_iov = &iov,
        .msg_iovlen = 1,
        .msg_control = &hdr,
        .msg_controllen = sizeof hdr,
    };
    res = recvmsg(fd, & msg, 0);
    if (res == -1) {
        return -errno;
    }
    for (struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg); cmsg != NULL; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
#ifdef IP_ORIGDSTADDR
        if (cmsg->cmsg_level == IPPROTO_IP && cmsg->cmsg_type == IP_ORIGDSTADDR) {
            struct sockaddr_in *da = (void *) CMSG_DATA(cmsg);
            memcpy(&dest, da, sizeof(struct sockaddr_in));
        } else
#endif
#ifdef IP_RECVDSTADDR
        if (cmsg->cmsg_level == IPPROTO_IP && cmsg->cmsg_type == IP_RECVDSTADDR) {
            struct sockaddr_in *da = (void *) CMSG_DATA(cmsg);
            memcpy(&dest, da, sizeof(struct sockaddr_in));
        } else
#endif
#ifdef IP_PKTINFO
        if (cmsg->cmsg_level == IPPROTO_IP && cmsg->cmsg_type == IP_PKTINFO) {
            struct in_pktinfo *pi = (void *) CMSG_DATA(cmsg);
            memcpy(&dest.addr_in.sin_addr, &pi->ipi_addr, sizeof(struct in_addr));
            // no port # in this case
        } else
#endif
#ifdef IPV6_PKTINFO
        if (cmsg->cmsg_level == IPPROTO_IPV6 && cmsg->cmsg_type == IPV6_PKTINFO) {
            struct in6_pktinfo *pi = (void *) CMSG_DATA(cmsg);
            memcpy(&dest.addr_in6.sin6_addr, &pi->ipi6_addr, sizeof(struct in6_addr));
            // no port # in this case
        } else
#endif
        continue;
    }

    if (res > 0) {
        position += res;
        (*env)->SetIntArrayRegion(env, posAndLimit, 0, 1, &position);
    }
    memcpy(&src, msg.msg_name, msg.msg_namelen);

    convert2(env, &src, srcAddrArray);
    convert2(env, &dest, destAddrArray);
    return res;
}

JNIEXPORT jlong JNICALL xnio_native(recvDirectScatter)(JNIEnv *env, jclass clazz, jint fd, jobjectArray bufferObjs, jint offs, jint len, jintArray posArray, jintArray limitArray) {
    struct iovec iov[len];
    ssize_t res;
    union sockaddr_any src = {};
    union sockaddr_any dest = {};
    union {
#ifdef IP_PKTINFO
        struct in_pktinfo pi;
#endif
#if defined(IP_RECVDSTADDR) || defined(IP_ORIGDSTADDR)
        struct sockaddr_in sa;
#endif
#if defined(IPV6_PKTINFO)
        struct in6_pktinfo pi6;
#endif
        char unused[48];
    } hdr;

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
        .msg_name = &src,
        .msg_namelen = sizeof src,
        .msg_iov = iov,
        .msg_iovlen = 1,
        .msg_control = &hdr,
        .msg_controllen = sizeof hdr,
    };
    res = recvmsg(fd, & msg, 0);
    if (res == -1) {
        return -errno;
    }
    for (struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg); cmsg != NULL; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
#ifdef IP_ORIGDSTADDR
        if (cmsg->cmsg_level == IPPROTO_IP && cmsg->cmsg_type == IP_ORIGDSTADDR) {
            struct sockaddr_in *da = (void *) CMSG_DATA(cmsg);
            memcpy(&dest, da, sizeof(struct sockaddr_in));
        } else
#endif
#ifdef IP_RECVDSTADDR
        if (cmsg->cmsg_level == IPPROTO_IP && cmsg->cmsg_type == IP_RECVDSTADDR) {
            struct sockaddr_in *da = (void *) CMSG_DATA(cmsg);
            memcpy(&dest, da, sizeof(struct sockaddr_in));
        } else
#endif
#ifdef IP_PKTINFO
        if (cmsg->cmsg_level == IPPROTO_IP && cmsg->cmsg_type == IP_PKTINFO) {
            struct in_pktinfo *pi = (void *) CMSG_DATA(cmsg);
            memcpy(&dest.addr_in.sin_addr, &pi->ipi_addr, sizeof(struct in_addr));
            // no port # in this case
        } else
#endif
#ifdef IPV6_PKTINFO
        if (cmsg->cmsg_level == IPPROTO_IPV6 && cmsg->cmsg_type == IPV6_PKTINFO) {
            struct in6_pktinfo *pi = (void *) CMSG_DATA(cmsg);
            memcpy(&dest.addr_in6.sin6_addr, &pi->ipi6_addr, sizeof(struct in6_addr));
            // no port # in this case
        } else
#endif
        continue;
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

JNIEXPORT jint JNICALL xnio_native(recvHeap)(JNIEnv *env, jclass clazz, jint fd, jbyteArray bufferObj, jintArray posAndLimit) {
    jint position, limit;
    union sockaddr_any src = {};
    union sockaddr_any dest = {};
    union {
#ifdef IP_PKTINFO
        struct in_pktinfo pi;
#endif
#if defined(IP_RECVDSTADDR) || defined(IP_ORIGDSTADDR)
        struct sockaddr_in sa;
#endif
#if defined(IPV6_PKTINFO)
        struct in6_pktinfo pi6;
#endif
        char unused[48];
    } hdr;
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
    struct iovec iov = {
        .iov_base = buffer,
        .iov_len = limit - position,
    };
    ssize_t res;
    struct msghdr msg = {
        .msg_name = &src,
        .msg_namelen = sizeof src,
        .msg_iov = &iov,
        .msg_iovlen = 1,
        .msg_control = &hdr,
        .msg_controllen = sizeof hdr,
    };
    res = recvmsg(fd, & msg, 0);
    if (res == -1) {
        (*env)->ReleaseByteArrayElements(env, bufferObj, buffer, JNI_ABORT);
        return -errno;
    }
    for (struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg); cmsg != NULL; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
#ifdef IP_ORIGDSTADDR
        if (cmsg->cmsg_level == IPPROTO_IP && cmsg->cmsg_type == IP_ORIGDSTADDR) {
            struct sockaddr_in *da = (void *) CMSG_DATA(cmsg);
            memcpy(&dest, da, sizeof(struct sockaddr_in));
        } else
#endif
#ifdef IP_RECVDSTADDR
        if (cmsg->cmsg_level == IPPROTO_IP && cmsg->cmsg_type == IP_RECVDSTADDR) {
            struct sockaddr_in *da = (void *) CMSG_DATA(cmsg);
            memcpy(&dest, da, sizeof(struct sockaddr_in));
        } else
#endif
#ifdef IP_PKTINFO
        if (cmsg->cmsg_level == IPPROTO_IP && cmsg->cmsg_type == IP_PKTINFO) {
            struct in_pktinfo *pi = (void *) CMSG_DATA(cmsg);
            memcpy(&dest.addr_in.sin_addr, &pi->ipi_addr, sizeof(struct in_addr));
            // no port # in this case
        } else
#endif
#ifdef IPV6_PKTINFO
        if (cmsg->cmsg_level == IPPROTO_IPV6 && cmsg->cmsg_type == IPV6_PKTINFO) {
            struct in6_pktinfo *pi = (void *) CMSG_DATA(cmsg);
            memcpy(&dest.addr_in6.sin6_addr, &pi->ipi6_addr, sizeof(struct in6_addr));
            // no port # in this case
        } else
#endif
        continue;
    }
    (*env)->ReleaseByteArrayElements(env, bufferObj, buffer, 0);
    if (res > 0) {
        position += res;
        (*env)->SetIntArrayRegion(env, posAndLimit, 0, 1, &position);
    }
    return res;
}

JNIEXPORT jlong JNICALL xnio_native(recvHeapScatter)(JNIEnv *env, jclass clazz, jint fd, jobjectArray bufferObjs, jint offs, jint len, jintArray posArray, jintArray limitArray) {
    struct iovec iov[len];
    ssize_t res;
    union sockaddr_any src = {};
    union sockaddr_any dest = {};
    union {
#ifdef IP_PKTINFO
        struct in_pktinfo pi;
#endif
#if defined(IP_RECVDSTADDR) || defined(IP_ORIGDSTADDR)
        struct sockaddr_in sa;
#endif
#if defined(IPV6_PKTINFO)
        struct in6_pktinfo pi6;
#endif
        char unused[48];
    } hdr;

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
        .msg_name = &src,
        .msg_namelen = sizeof src,
        .msg_iov = iov,
        .msg_iovlen = 1,
        .msg_control = &hdr,
        .msg_controllen = sizeof hdr,
    };
    res = recvmsg(fd, & msg, 0);
    if (res == -1) {
        return -errno;
    }
    for (struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg); cmsg != NULL; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
#ifdef IP_ORIGDSTADDR
        if (cmsg->cmsg_level == IPPROTO_IP && cmsg->cmsg_type == IP_ORIGDSTADDR) {
            struct sockaddr_in *da = (void *) CMSG_DATA(cmsg);
            memcpy(&dest, da, sizeof(struct sockaddr_in));
        } else
#endif
#ifdef IP_RECVDSTADDR
        if (cmsg->cmsg_level == IPPROTO_IP && cmsg->cmsg_type == IP_RECVDSTADDR) {
            struct sockaddr_in *da = (void *) CMSG_DATA(cmsg);
            memcpy(&dest, da, sizeof(struct sockaddr_in));
        } else
#endif
#ifdef IP_PKTINFO
        if (cmsg->cmsg_level == IPPROTO_IP && cmsg->cmsg_type == IP_PKTINFO) {
            struct in_pktinfo *pi = (void *) CMSG_DATA(cmsg);
            memcpy(&dest.addr_in.sin_addr, &pi->ipi_addr, sizeof(struct in_addr));
            // no port # in this case
        } else
#endif
#ifdef IPV6_PKTINFO
        if (cmsg->cmsg_level == IPPROTO_IPV6 && cmsg->cmsg_type == IPV6_PKTINFO) {
            struct in6_pktinfo *pi = (void *) CMSG_DATA(cmsg);
            memcpy(&dest.addr_in6.sin6_addr, &pi->ipi6_addr, sizeof(struct in6_addr));
            // no port # in this case
        } else
#endif
        continue;
    }

    jlong ret = res;
    jint n;
    for (int i = 0; i < len; i++) {
        jbyteArray array = (jbyteArray) (*env)->GetObjectArrayElement(env, bufferObjs, i);
        (*env)->ReleaseByteArrayElements(env, array, (jbyte *) iov[i].iov_base, 0);
        size_t s = iov[i].iov_len;
        n = s > res ? res : s;
        res -= s;
        (*env)->SetIntArrayRegion(env, posArray, i, 1, &n);
    }
    return ret;

fail2:
    (*env)->ReleasePrimitiveArrayCritical(env, limitArray, limit, JNI_ABORT);
fail1:
    (*env)->ReleasePrimitiveArrayCritical(env, posArray, pos, JNI_ABORT);
fail0:
    return res;
}
