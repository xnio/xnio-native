/*
 * JBoss, Home of Professional Open Source
 *
 * Copyright 2013 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.xnio.nativeimpl;

import java.io.IOException;
import java.util.Set;

import org.xnio.Option;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class UnixConnection extends NativeStreamConnection {

    protected UnixConnection(final NativeWorkerThread thread, final int fd) {
        super(thread, fd);
    }

    protected NativeStreamConduit constructConduit(final NativeWorkerThread thread, final int fd) {
        return new UnixStreamConduit(thread, fd, this);
    }

    private static final Set<Option<?>> OPTIONS = Option.setBuilder()
            .create();

    public boolean supportsOption(final Option<?> option) {
        return OPTIONS.contains(option) || super.supportsOption(option);
    }

    public <T> T getOption(final Option<T> option) throws IOException {
        return super.getOption(option);
    }

    public <T> T setOption(final Option<T> option, final T value) throws IllegalArgumentException, IOException {
        return super.setOption(option, value);
    }
}
