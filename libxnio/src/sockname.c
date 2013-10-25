
#include "xnio.h"

#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <sys/un.h>
#include <string.h>
#include <stdbool.h>

jbyteArray convert(JNIEnv *env, union sockaddr_any *addr) {
    jbyteArray result;
    jbyte type;
    switch (addr->addr.sa_family) {
        case AF_INET: {
            type = 0;
            result = (*env)->NewByteArray(env, 7);
            if (result == NULL) {
                return 0;
            }
            (*env)->SetByteArrayRegion(env, result, 0, 1, &type);
            (*env)->SetByteArrayRegion(env, result, 1, 4, (jbyte *)&addr->addr_in.sin_addr);
            (*env)->SetByteArrayRegion(env, result, 5, 2, (jbyte *)&addr->addr_in.sin_port);
            break;
        }
        case AF_INET6: {
            type = 1;
            result = (*env)->NewByteArray(env, 19);
            if (result == NULL) {
                return 0;
            }
            (*env)->SetByteArrayRegion(env, result, 0, 1, &type);
            (*env)->SetByteArrayRegion(env, result, 1, 16, (jbyte *)&addr->addr_in6.sin6_addr);
            (*env)->SetByteArrayRegion(env, result, 17, 2, (jbyte *)&addr->addr_in6.sin6_port);
            break;
        }
        case AF_UNIX: {
            jbyte size;
            void *start, *end;
            start = &addr->addr_un.sun_path;
            end = memchr(start, 0, sizeof addr->addr_un.sun_path);
            type = 2;
            size = end - start;
            result = (*env)->NewByteArray(env, 2 + size);
            if (result == NULL) {
                return 0;
            }
            (*env)->SetByteArrayRegion(env, result, 0, 1, &type);
            (*env)->SetByteArrayRegion(env, result, 1, 1, &size);
            (*env)->SetByteArrayRegion(env, result, 2, size, (jbyte *) &start);
            break;
        }
        default: {
            return 0;
        }
    }
    return result;
}

void convert2(JNIEnv *env, union sockaddr_any *addr, jbyteArray result) {
    jbyte type;
    switch (addr->addr.sa_family) {
        case AF_INET: {
            type = 0;
            (*env)->SetByteArrayRegion(env, result, 0, 1, &type);
            (*env)->SetByteArrayRegion(env, result, 1, 4, (jbyte *)&addr->addr_in.sin_addr);
            (*env)->SetByteArrayRegion(env, result, 5, 2, (jbyte *)&addr->addr_in.sin_port);
            break;
        }
        case AF_INET6: {
            type = 1;
            (*env)->SetByteArrayRegion(env, result, 0, 1, &type);
            (*env)->SetByteArrayRegion(env, result, 1, 16, (jbyte *)&addr->addr_in6.sin6_addr);
            (*env)->SetByteArrayRegion(env, result, 17, 2, (jbyte *)&addr->addr_in6.sin6_port);
            break;
        }
        case AF_UNIX: {
            jbyte size;
            void *start, *end;
            start = &addr->addr_un.sun_path;
            end = memchr(start, 0, sizeof addr->addr_un.sun_path);
            type = 2;
            size = end - start;
            (*env)->SetByteArrayRegion(env, result, 0, 1, &type);
            (*env)->SetByteArrayRegion(env, result, 1, 1, &size);
            (*env)->SetByteArrayRegion(env, result, 2, size, (jbyte *) &start);
            break;
        }
        default: {
            type = -1;
            (*env)->SetByteArrayRegion(env, result, 0, 1, &type);
        }
    }
}

jint decode(JNIEnv *env, jbyteArray src, union sockaddr_any *dest) {
    jbyte *bytes = (*env)->GetPrimitiveArrayCritical(env, src, 0);
    if (! bytes) { return -ENOMEM; }
    switch (bytes[0]) {
        case 0: // ipv4
            dest->addr_in.sin_family = AF_INET;
            memcpy(&dest->addr_in.sin_addr, bytes + 1, 4);
            memcpy(&dest->addr_in.sin_port, bytes + 5, 2);
            return 0;
        case 1: // ipv6
            dest->addr_in6.sin6_family = AF_INET6;
            memcpy(&dest->addr_in6.sin6_addr, bytes + 1, 16);
            memcpy(&dest->addr_in6.sin6_port, bytes + 17, 2);
            memcpy(&dest->addr_in6.sin6_scope_id, bytes + 19, 4);
            return 0;
        case 2: // unix
            dest->addr_un.sun_family = AF_UNIX;
            memcpy(&dest->addr_un.sun_path, bytes + 1, sizeof dest->addr_un.sun_path);
            return 0;
        default:
            return -EINVAL;
    }
}

JNIEXPORT jbyteArray JNICALL xnio_native(getSockName)(JNIEnv *env, jclass clazz, jint fd) {
    union sockaddr_any addr;
    socklen_t addrlen = sizeof addr;
    if (getsockname(fd, &addr.addr, &addrlen) < 0) {
        return 0;
    }
    if (addrlen > sizeof addr) {
        return 0;
    }
    return convert(env, &addr);
}

JNIEXPORT jbyteArray JNICALL xnio_native(getPeerName)(JNIEnv *env, jclass clazz, jint fd) {
    union sockaddr_any addr;
    socklen_t addrlen = sizeof addr;
    if (getpeername(fd, &addr.addr, &addrlen) < 0) {
        return 0;
    }
    if (addrlen > sizeof addr) {
        return 0;
    }
    return convert(env, &addr);
}
