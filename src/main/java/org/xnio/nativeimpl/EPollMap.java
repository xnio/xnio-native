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

import static org.xnio.Bits.allAreSet;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Integer-indexed hash map.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class EPollMap extends AbstractCollection<EPollRegistration> {
    private static final int DEFAULT_INITIAL_CAPACITY = 512;
    private static final int MAXIMUM_CAPACITY = 1 << 30;
    private static final float DEFAULT_LOAD_FACTOR = 0.60f;

    private final float loadFactor;
    private final int initialCapacity;

    private EPollRegistration[][] table;
    int size;
    int threshold;

    EPollMap() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Construct a new instance.
     *
     * @param initialCapacity the initial capacity
     * @param loadFactor the load factor
     */
    EPollMap(int initialCapacity, float loadFactor) {
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
        threshold = (int) (capacity * loadFactor);

        table = new EPollRegistration[capacity][];
    }

    public EPollRegistration putIfAbsent(final EPollRegistration value) {
        return doPut(value, true);
    }

    public EPollRegistration removeKey(final int index) {
        return doRemove(index);
    }

    public boolean remove(final Object value) {
        return value instanceof EPollRegistration && doRemove((EPollRegistration) value);
    }

    public boolean remove(final EPollRegistration value) {
        return value != null && doRemove(value);
    }

    public boolean containsKey(final int index) {
        return doGet(index) != null;
    }

    public EPollRegistration get(final int index) {
        return doGet(index);
    }

    public EPollRegistration put(final EPollRegistration value) {
        if (value == null) {
            throw new IllegalArgumentException("value is null");
        }
        return doPut(value, false);
    }

    public EPollRegistration replace(final EPollRegistration value) {
        if (value == null) {
            throw new IllegalArgumentException("value is null");
        }
        return doReplace(value);
    }

    public boolean replace(final EPollRegistration oldValue, final EPollRegistration newValue) {
        if (newValue == null) {
            throw new IllegalArgumentException("newValue is null");
        }
        if (oldValue.id != newValue.id) {
            throw new IllegalArgumentException("Can only replace with value which has the same key");
        }
        return doReplace(oldValue, newValue);
    }

    public int getKey(final EPollRegistration argument) {
        return argument.id;
    }

    public boolean add(final EPollRegistration value) {
        if (value == null) {
            throw new IllegalArgumentException("value is null");
        }
        return doPut(value, true) == null;
    }

    @SuppressWarnings("unchecked")
    public <T> T[] toArray(final T[] a) {
        final T[] target;
        if (a.length < size) {
            target = Arrays.copyOfRange(a, a.length, a.length + size);
        } else {
            target = a;
        }
        int i = 0;
        for (final EPollRegistration[] row : table) {
            if (row != null) {
                for (EPollRegistration item : row) {
                    if (item != null) {
                        target[i++] = (T) item;
                    }
                }
            }
        }
        return target;
    }

    public Object[] toArray() {
        final ArrayList<Object> list = new ArrayList<Object>(size());
        list.addAll(this);
        return list.toArray();
    }

    public boolean contains(final Object o) {
        return o instanceof EPollRegistration && o == get(((EPollRegistration) o).id);
    }

    public Iterator<EPollRegistration> iterator() {
        return new EntryIterator();
    }

    public int size() {
        return size;
    }

    private boolean doReplace(final EPollRegistration oldValue, final EPollRegistration newValue) {
        if (oldValue.id != newValue.id) {
            return false;
        }
        EPollRegistration[][] table = this.table;
        final int idx = oldValue.id & table.length - 1;

        EPollRegistration[] row = table[idx];
        if (row == null) {
            return false;
        }
        int rowLength = row.length;
        for (int i = 0; i < rowLength; i++) {
            EPollRegistration item = row[i];
            if (item != null && item == oldValue) {
                row[i] = newValue;
                return true;
            }
        }
        return false;
    }

    private EPollRegistration doReplace(final EPollRegistration value) {
        EPollRegistration[][] table = this.table;
        final int idx = value.id & table.length - 1;

        EPollRegistration[] row = table[idx];
        if (row == null) {
            return null;
        }
        int rowLength = row.length;
        for (int i = 0; i < rowLength; i++) {
            EPollRegistration item = row[i];
            if (item != null && item.id == value.id) {
                row[i] = value;
                return item;
            }
        }
        return null;
    }

    private boolean doRemove(final EPollRegistration value) {
        EPollRegistration[][] table = this.table;
        final int idx = value.id & table.length - 1;

        EPollRegistration[] row = table[idx];
        if (row == null) {
            return false;
        }
        int rowLength = row.length;
        for (int i = 0; i < rowLength; i++) {
            EPollRegistration item = row[i];
            if (item != null && item == value) {
                row[i] = null;
                size--;
                return true;
            }
        }
        return false;
    }

    private EPollRegistration doRemove(final int key) {
        EPollRegistration[][] table = this.table;
        final int idx = key & table.length - 1;

        EPollRegistration[] row = table[idx];
        if (row == null) {
            return null;
        }
        int rowLength = row.length;
        for (int i = 0; i < rowLength; i++) {
            EPollRegistration item = row[i];
            if (item != null && item.id == key) {
                row[i] = null;
                size--;
                return item;
            }
        }
        return null;
    }

    private EPollRegistration doPut(EPollRegistration value, boolean ifAbsent) {
        final int hashCode = value.id;
        EPollRegistration[][] table = this.table;
        final int idx = hashCode & table.length - 1;

        EPollRegistration[] row = table[idx];
        if (row == null) {
            grow();
            table[idx] = new EPollRegistration[3];
            table[idx][0] = value;
            return null;
        }
        int s = -1;
        int rowLength = row.length;
        for (int i = 0; i < rowLength; i++) {
            EPollRegistration item = row[i];
            if (item == null) {
                if (s == -1) {
                    s = i;
                }
            } else if (item.id == value.id) {
                if (! ifAbsent) {
                    row[i] = value;
                }
                return item;
            }
        }
        if (s != -1) {
            grow();
            row[s] = value;
            return null;
        }
        grow();
        row = Arrays.copyOf(row, rowLength + 2);
        row[rowLength] = value;
        table[idx] = row;
        return null;
    }

    private void grow() {
        if (size == Integer.MAX_VALUE) {
            throw new IllegalStateException("Table full");
        }
        if (size ++ < threshold || threshold == Integer.MAX_VALUE) {
            return;
        }
        final EPollRegistration[][] oldTable = table;
        final int oldLen = oldTable.length;
        assert Integer.bitCount(oldLen) == 1;
        final EPollRegistration[][] newTable = Arrays.copyOf(oldTable, oldLen << 1);
        for (int i = 0; i < oldLen; i++) {
            EPollRegistration[] row = newTable[i];
            EPollRegistration[] newRow = row.clone();
            newTable[i + oldLen] = newRow;
            final int rowLen = row.length;
            for (int j = 0; j < rowLen; j++) {
                final EPollRegistration item = row[j];
                if (item != null) {
                    if (allAreSet(item.id, oldLen)) {
                        row[j] = null;
                    } else {
                        newRow[j] = null;
                    }
                }
            }
        }
        table = newTable;
        threshold = (int) (newTable.length * loadFactor);
    }

    private EPollRegistration doGet(final int key) {
        final EPollRegistration[][] table = this.table;
        int idx = key & (table.length - 1);
        final EPollRegistration[] row = table[idx];
        if (row != null) for (EPollRegistration item : row) {
            if (item != null && key == item.id) {
                return item;
            }
        }
        return null;
    }

    public void clear() {
        table = new EPollRegistration[initialCapacity][];
        size = 0;
        threshold = (int) (initialCapacity * loadFactor);
    }

    class EntryIterator implements Iterator<EPollRegistration> {
        int rowIdx;
        int itemIdx;
        EPollRegistration next;

        public boolean hasNext() {
            if (next == null) {
                while (rowIdx < table.length) {
                    final EPollRegistration[] row = table[rowIdx];
                    if (row != null) {
                        while (itemIdx < row.length) {
                            final EPollRegistration item = row[itemIdx++];
                            if (item != null) {
                                next = item;
                                return true;
                            }
                        }
                        itemIdx = 0;
                        rowIdx++;
                    }
                }
                return false;
            } else {
                return true;
            }
        }

        public EPollRegistration next() {
            if (! hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                return next;
            } finally {
                next = null;
            }
        }

        public void remove() {
            table[rowIdx][itemIdx - 1] = null;
            size--;
        }
    }
}
