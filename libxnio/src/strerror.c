
#include "xnio.h"

#include <string.h>
#include <stdlib.h>

JNIEXPORT jstring JNICALL xnio_native(strError)(JNIEnv *env, jclass clazz, jint error) {
    char buf[256];
    int code = abs(error);
    char *result = strerror_r(code, buf, sizeof buf);
    if (! result) {
        return 0;
    }
    return (*env)->NewStringUTF(env, result);
}
