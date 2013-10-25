/*
 * JBoss, Home of Professional Open Source
 *
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
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

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Lock-free concurrent integer-indexed hash map.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class FdMap extends AbstractCollection<NativeRawChannel<?>> {
    private static final int DEFAULT_INITIAL_CAPACITY = 512;
    private static final int MAXIMUM_CAPACITY = 1 << 30;
    private static final float DEFAULT_LOAD_FACTOR = 0.60f;

    /** A row which has been resized into the new view. */
    private static final NativeRawChannel<?>[] RESIZED = new NativeRawChannel<?>[0];

    private volatile Table table;

    private final float loadFactor;
    private final int initialCapacity;

    @SuppressWarnings("unchecked")
    private static final AtomicIntegerFieldUpdater<Table> sizeUpdater = AtomicIntegerFieldUpdater.newUpdater(Table.class, "size");

    @SuppressWarnings("unchecked")
    private static final AtomicReferenceFieldUpdater<FdMap, Table> tableUpdater = AtomicReferenceFieldUpdater.newUpdater(FdMap.class, Table.class, "table");

    FdMap() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Construct a new instance.
     *
     * @param indexer the key indexer
     * @param valueEqualler the value equaller
     * @param initialCapacity the initial capacity
     * @param loadFactor the load factor
     */
    FdMap(int initialCapacity, float loadFactor) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Initial capacity must be > 0");
        }
        if (initialCapacity > MAXIMUM_CAPACITY) {
            initialCapacity = MAXIMUM_CAPACITY;
        }
        if (loadFactor <= 0.0 || Float.isNaN(loadFactor) || loadFactor >= 1.0) {
            throw new IllegalArgumentException("Load factor must be between 0.0f and 1.0f");
        }

        int capacity = 1;

        while (capacity < initialCapacity) {
            capacity <<= 1;
        }

        this.loadFactor = loadFactor;
        this.initialCapacity = capacity;

        final Table table = new Table(capacity, loadFactor);
        tableUpdater.set(this, table);
    }

    public NativeRawChannel<?> putIfAbsent(final NativeRawChannel<?> value) {
        final NativeRawChannel<?> result = doPut(value, true, table);
        return result;
    }

    public NativeRawChannel<?> removeKey(final int index) {
        return doRemove(index, table);
    }

    public boolean remove(final Object value) {
        return doRemove((NativeRawChannel<?>) value, table);
    }

    public boolean containsKey(final int index) {
        return doGet(table, index) != null;
    }

    public NativeRawChannel<?> get(final int index) {
        return doGet(table, index);
    }

    public NativeRawChannel<?> put(final NativeRawChannel<?> value) {
        if (value == null) {
            throw new IllegalArgumentException("value is null");
        }
        final NativeRawChannel<?> result = doPut(value, false, table);
        return result;
    }

    public NativeRawChannel<?> replace(final NativeRawChannel<?> value) {
        if (value == null) {
            throw new IllegalArgumentException("value is null");
        }
        final NativeRawChannel<?> result = doReplace(value, table);
        return result;
    }

    public boolean replace(final NativeRawChannel<?> oldValue, final NativeRawChannel<?> newValue) {
        if (newValue == null) {
            throw new IllegalArgumentException("newValue is null");
        }
        if (oldValue.fd != newValue.fd) {
            throw new IllegalArgumentException("Can only replace with value which has the same key");
        }
        return doReplace(oldValue, newValue, table);
    }

    public int getKey(final NativeRawChannel<?> argument) {
        return argument.fd;
    }

    public boolean add(final NativeRawChannel<?> value) {
        if (value == null) {
            throw new IllegalArgumentException("value is null");
        }
        return doPut(value, true, table) == null;
    }

    public <T> T[] toArray(final T[] a) {
        final ArrayList<T> list = new ArrayList<T>(size());
        list.addAll((Collection<T>) this);
        return list.toArray(a);
    }

    public Object[] toArray() {
        final ArrayList<Object> list = new ArrayList<Object>(size());
        list.addAll(this);
        return list.toArray();
    }

    public boolean contains(final Object o) {
        return o instanceof NativeRawChannel && o == get(((NativeRawChannel<?>) o).fd);
    }

    public Iterator<NativeRawChannel<?>> iterator() {
        return new EntryIterator();
    }

    public int size() {
        return table.size & 0x7fffffff;
    }

    private boolean doReplace(final NativeRawChannel<?> oldValue, final NativeRawChannel<?> newValue, final Table table) {
        final int key = oldValue.fd;
        final AtomicReferenceArray<NativeRawChannel<?>[]> array = table.array;
        final int idx = key & array.length() - 1;

        OUTER: for (;;) {
            // Fetch the table row.
            NativeRawChannel<?>[] oldRow = array.get(idx);
            if (oldRow == null) {
                // no match for the key
                return false;
            }
            if (oldRow == RESIZED) {
                return doReplace(oldValue, newValue, table.resizeView);
            }

            for (int i = 0, length = oldRow.length; i < length; i++) {
                final Object tryItem = oldRow[i];
                if (tryItem == oldValue) {
                    final NativeRawChannel<?>[] newRow = oldRow.clone();
                    newRow[i] = newValue;
                    if (array.compareAndSet(i, oldRow, newRow)) {
                        return true;
                    } else {
                        continue OUTER;
                    }
                }
            }
            return false;
        }
    }

    private NativeRawChannel<?> doReplace(final NativeRawChannel<?> value, final Table table) {
        final int key = value.fd;
        final AtomicReferenceArray<NativeRawChannel<?>[]> array = table.array;
        final int idx = key & array.length() - 1;

        OUTER: for (;;) {
            // Fetch the table row.
            NativeRawChannel<?>[] oldRow = array.get(idx);
            if (oldRow == null) {
                // no match for the key
                return null;
            }
            if (oldRow == RESIZED) {
                return doReplace(value, table.resizeView);
            }

            // Find the matching Item in the row.
            for (int i = 0, length = oldRow.length; i < length; i++) {
                final NativeRawChannel<?> tryItem = oldRow[i];
                if (key == tryItem.fd) {
                    final NativeRawChannel<?>[] newRow = oldRow.clone();
                    newRow[i] = value;
                    if (array.compareAndSet(i, oldRow, newRow)) {
                        return tryItem;
                    } else {
                        continue OUTER;
                    }
                }
            }
            return null;
        }
    }

    private boolean doRemove(final NativeRawChannel<?> item, final Table table) {
        int key = item.fd;

        final AtomicReferenceArray<NativeRawChannel<?>[]> array = table.array;
        final int idx = key & array.length() - 1;

        NativeRawChannel<?>[] oldRow;

        OUTER: for (;;) {
            oldRow = array.get(idx);
            if (oldRow == null) {
                return false;
            }
            if (oldRow == RESIZED) {
                boolean result;
                if (result = doRemove(item, table.resizeView)) {
                    sizeUpdater.getAndDecrement(table);
                }
                return result;
            }

            for (int i = 0; i < oldRow.length; i ++) {
                if (item == oldRow[i]) {
                    if (array.compareAndSet(idx, oldRow, remove(oldRow, i))) {
                        sizeUpdater.getAndDecrement(table);
                        return true;
                    } else {
                        continue OUTER;
                    }
                }
            }
            // not found
            return false;
        }
    }

    private NativeRawChannel<?> doRemove(final int key, final Table table) {
        final AtomicReferenceArray<NativeRawChannel<?>[]> array = table.array;
        final int idx = key & array.length() - 1;

        NativeRawChannel<?>[] oldRow;

        OUTER: for (;;) {
            oldRow = array.get(idx);
            if (oldRow == null) {
                return null;
            }
            if (oldRow == RESIZED) {
                Object result;
                if ((result = doRemove(key, table.resizeView)) != null) {
                    sizeUpdater.getAndDecrement(table);
                }
                return (NativeRawChannel<?>) result;
            }

            for (int i = 0; i < oldRow.length; i ++) {
                if (key == oldRow[i].fd) {
                    if (array.compareAndSet(idx, oldRow, remove(oldRow, i))) {
                        sizeUpdater.getAndDecrement(table);
                        return oldRow[i];
                    } else {
                        continue OUTER;
                    }
                }
            }
            // not found
            return null;
        }
    }

    private NativeRawChannel<?> doPut(NativeRawChannel<?> value, boolean ifAbsent, Table table) {
        final int hashCode = value.fd;
        final AtomicReferenceArray<NativeRawChannel<?>[]> array = table.array;
        final int idx = hashCode & array.length() - 1;

        OUTER: for (;;) {

            // Fetch the table row.
            NativeRawChannel<?>[] oldRow = array.get(idx);
            if (oldRow == RESIZED) {
                // row was transported to the new table so recalculate everything
                final Object result = doPut(value, ifAbsent, table.resizeView);
                // keep a consistent size view though!
                if (result == null) sizeUpdater.getAndIncrement(table);
                return (NativeRawChannel<?>) result;
            }
            if (oldRow != null) {
                // Find the matching Item in the row.
                NativeRawChannel<?> oldItem;
                for (int i = 0, length = oldRow.length; i < length; i++) {
                    if (hashCode == oldRow[i].fd) {
                        if (ifAbsent) {
                            return oldRow[i];
                        } else {
                            NativeRawChannel<?>[] newRow = oldRow.clone();
                            newRow[i] = value;
                            oldItem = oldRow[i];
                            if (array.compareAndSet(idx, oldRow, newRow)) {
                                return oldItem;
                            } else {
                                // retry
                                continue OUTER;
                            }
                        }
                    }
                }
            }

            if (array.compareAndSet(idx, oldRow, addItem(oldRow, value))) {
                // Up the table size.
                final int threshold = table.threshold;
                int newSize = sizeUpdater.incrementAndGet(table);
                // >= 0 is really a sign-bit check
                while (newSize >= 0 && (newSize & 0x7fffffff) > threshold) {
                    if (sizeUpdater.compareAndSet(table, newSize, newSize | 0x80000000)) {
                        resize(table);
                        return null;
                    }
                }
                // Success.
                return null;
            }
        }
    }

    private void resize(Table origTable) {
        final AtomicReferenceArray<NativeRawChannel<?>[]> origArray = origTable.array;
        final int origCapacity = origArray.length();
        final Table newTable = new Table(origCapacity << 1, loadFactor);
        // Prevent resize until we're done...
        newTable.size = 0x80000000;
        origTable.resizeView = newTable;
        final AtomicReferenceArray<NativeRawChannel<?>[]> newArray = newTable.array;

        for (int i = 0; i < origCapacity; i ++) {
            // for each row, try to resize into two new rows
            NativeRawChannel<?>[] origRow, newRow0, newRow1;
            do {
                origRow = origArray.get(i);
                if (origRow != null) {
                    int count0 = 0, count1 = 0;
                    for (NativeRawChannel<?> item : origRow) {
                        if ((item.fd & origCapacity) == 0) {
                            count0++;
                        } else {
                            count1++;
                        }
                    }
                    if (count0 != 0) {
                        newRow0 = new NativeRawChannel<?>[count0];
                        int j = 0;
                        for (NativeRawChannel<?> item : origRow) {
                            if ((item.fd & origCapacity) == 0) {
                                newRow0[j++] = item;
                            }
                        }
                        newArray.lazySet(i, newRow0);
                    }
                    if (count1 != 0) {
                        newRow1 = new NativeRawChannel<?>[count1];
                        int j = 0;
                        for (NativeRawChannel<?> item : origRow) {
                            if ((item.fd & origCapacity) != 0) {
                                newRow1[j++] = item;
                            }
                        }
                        newArray.lazySet(i + origCapacity, newRow1);
                    }
                }
            } while (! origArray.compareAndSet(i, origRow, RESIZED));
            sizeUpdater.getAndAdd(newTable, origRow.length);
        }

        int size;
        do {
            size = newTable.size;
            if ((size & 0x7fffffff) >= newTable.threshold) {
                // shorter path for reads and writes
                table = newTable;
                // then time for another resize, right away
                resize(newTable);
                return;
            }
        } while (!sizeUpdater.compareAndSet(newTable, size, size & 0x7fffffff));

        // All done, plug in the new table
        table = newTable;
    }

    private static NativeRawChannel<?>[] remove(NativeRawChannel<?>[] row, int idx) {
        final int len = row.length;
        assert idx < len;
        if (len == 1) {
            return null;
        }
        NativeRawChannel<?>[] newRow = new NativeRawChannel<?>[len - 1];
        if (idx > 0) {
            System.arraycopy(row, 0, newRow, 0, idx);
        }
        if (idx < len - 1) {
            System.arraycopy(row, idx + 1, newRow, idx, len - 1 - idx);
        }
        return newRow;
    }

    private NativeRawChannel<?> doGet(final Table table, final int key) {
        final AtomicReferenceArray<NativeRawChannel<?>[]> array = table.array;
        final NativeRawChannel<?>[] row = array.get(key & (array.length() - 1));
        if (row != null) for (NativeRawChannel<?> item : row) {
            if (key == item.fd) {
                return item;
            }
        }
        return null;
    }

    public void clear() {
        table = new Table(initialCapacity, loadFactor);
    }

    private static NativeRawChannel<?>[] addItem(final NativeRawChannel<?>[] row, final NativeRawChannel<?> newItem) {
        if (row == null) {
            return new NativeRawChannel<?>[] { newItem };
        } else {
            final int length = row.length;
            NativeRawChannel<?>[] newRow = Arrays.copyOf(row, length + 1);
            newRow[length] = newItem;
            return newRow;
        }
    }

    final class RowIterator implements Iterator {
        private final Table table;
        NativeRawChannel<?>[] row;

        private int idx;
        private int removeIdx = -1;
        private NativeRawChannel<?> next;

        RowIterator(final Table table, final NativeRawChannel<?>[] row) {
            this.table = table;
            this.row = row;
        }

        public boolean hasNext() {
            while (next == null) {
                final NativeRawChannel<?>[] row = this.row;
                if (row == null || idx == row.length) {
                    return false;
                }
                next = row[idx++];
            }
            return true;
        }

        public NativeRawChannel<?> next() {
            if (hasNext()) try {
                removeIdx = idx - 1;
                return next;
            } finally {
                next = null;
            }
            throw new NoSuchElementException();
        }

        public void remove() {
            int removeIdx = this.removeIdx;
            this.removeIdx = -1;
            if (removeIdx == -1) {
                throw new IllegalStateException("next() not yet called");
            }
            doRemove(row[removeIdx], table);
        }
    }

    final class BranchIterator implements Iterator<NativeRawChannel<?>> {
        private final Iterator<NativeRawChannel<?>> branch0;
        private final Iterator<NativeRawChannel<?>> branch1;

        private boolean branch;

        BranchIterator(final Iterator<NativeRawChannel<?>> branch0, final Iterator<NativeRawChannel<?>> branch1) {
            this.branch0 = branch0;
            this.branch1 = branch1;
        }

        public boolean hasNext() {
            return branch0.hasNext() || branch1.hasNext();
        }

        public NativeRawChannel<?> next() {
            if (branch) {
                return branch1.next();
            }
            if (branch0.hasNext()) {
                return branch0.next();
            } else {
                branch = true;
                return branch1.next();
            }
        }

        public void remove() {
            if (branch) {
                branch0.remove();
            } else {
                branch1.remove();
            }
        }
    }

    private Iterator<NativeRawChannel<?>> createRowIterator(Table table, int rowIdx) {
        final AtomicReferenceArray<NativeRawChannel<?>[]> array = table.array;
        final NativeRawChannel<?>[] row = array.get(rowIdx);
        if (row == RESIZED) {
            final Table resizeView = table.resizeView;
            return new BranchIterator(createRowIterator(resizeView, rowIdx), createRowIterator(resizeView, rowIdx + array.length()));
        } else {
            return new RowIterator(table, row);
        }
    }

    final class EntryIterator implements Iterator<NativeRawChannel<?>> {
        private final Table table = FdMap.this.table;
        private Iterator<NativeRawChannel<?>> tableIterator;
        private Iterator<NativeRawChannel<?>> removeIterator;
        private int tableIdx;
        private NativeRawChannel<?> next;

        public boolean hasNext() {
            while (next == null) {
                if (tableIdx == table.array.length()) {
                    return false;
                }
                if (tableIterator == null) {
                    tableIterator = createRowIterator(table, tableIdx++);
                }
                if (tableIterator.hasNext()) {
                    next = tableIterator.next();
                    return true;
                } else {
                    tableIterator = null;
                }
            }
            return true;
        }

        public NativeRawChannel<?> next() {
            if (hasNext()) try {
                return next;
            } finally {
                removeIterator = tableIterator;
                next = null;
            }
            throw new NoSuchElementException();
        }

        public void remove() {
            final Iterator<NativeRawChannel<?>> removeIterator = this.removeIterator;
            if (removeIterator == null) {
                throw new IllegalStateException();
            } else try {
                removeIterator.remove();
            } finally {
                this.removeIterator = null;
            }
        }
    }

    static final class Table {
        final AtomicReferenceArray<NativeRawChannel<?>[]> array;
        final int threshold;
        /** Bits 0-30 are size; bit 31 is 1 if the table is being resized. */
        volatile int size;
        volatile Table resizeView;

        private Table(int capacity, float loadFactor) {
            array = new AtomicReferenceArray<NativeRawChannel<?>[]>(capacity);
            threshold = capacity == MAXIMUM_CAPACITY ? Integer.MAX_VALUE : (int)(capacity * loadFactor);
        }
    }
}
