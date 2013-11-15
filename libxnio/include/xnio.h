/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

#define _GNU_SOURCE

#include <jni.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <sys/un.h>
#include <stdbool.h>

#define xnio_native(name) Java_org_xnio_nativeimpl_Native_##name

// Use "weak" to redeclare optional features
#define weak __attribute__((weak))

// Our "universal" socket address structure

union sockaddr_any {
    struct sockaddr addr;
    struct sockaddr_un addr_un;
    struct sockaddr_in addr_in;
    struct sockaddr_in6 addr_in6;
};

extern jbyteArray convert(JNIEnv *env, union sockaddr_any *addr);

extern void convert2(JNIEnv *env, union sockaddr_any *addr, jbyteArray target);

extern jint decode(JNIEnv *env, jbyteArray src, union sockaddr_any *dest);

#define Buffer_pos org_xnio_Buffer_pos
#define Buffer_lim org_xnio_Buffer_lim

#define ByteBuffer_array org_xnio_ByteBuffer_array
#define ByteBuffer_offset org_xnio_ByteBuffer_offset

#define FileChannelImpl_fd org_xnio_FileChannelImpl_fd
#define FileDescriptor_fd org_xnio_FileDescriptor_fd

extern jfieldID Buffer_pos;
extern jfieldID Buffer_lim;

extern jfieldID ByteBuffer_array;
extern jfieldID ByteBuffer_offset;

extern jfieldID FileChannelImpl_fd;
extern jfieldID FileDescriptor_fd;

#define XNIO_EPOLL_READ    0x01
#define XNIO_EPOLL_WRITE   0x02
#define XNIO_EPOLL_EDGE    0x04
