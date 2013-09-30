/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.collect.Maps;

class MapLeaf extends MapRecord {

    private final String[] keys;

    MapLeaf(Segment segment, int offset, int size, int level) {
        super(segment, offset, size, level);
        checkArgument(size != 0 || level == 0);
        checkArgument(size <= BUCKETS_PER_LEVEL || level == MAX_NUMBER_OF_LEVELS);
        this.keys = new String[size];
    }

    MapLeaf(Segment segment, RecordId id, int size, int level) {
        super(segment, id, size, level);
        checkArgument(size != 0 || level == 0);
        checkArgument(size <= BUCKETS_PER_LEVEL || level == MAX_NUMBER_OF_LEVELS);
        this.keys = new String[size];
    }

    Map<String, MapEntry> getMapEntries() {
        RecordId[] keys = new RecordId[size];
        RecordId[] values = new RecordId[size];

        Segment segment = getSegment();
        int bytes = 4 + size * 4;
        int ids = 0;
        for (int i = 0; i < size; i++) {
            keys[i] = segment.readRecordId(getOffset(bytes, ids++));
        }
        for (int i = 0; i < size; i++) {
            values[i] = segment.readRecordId(getOffset(bytes, ids++));
        }

        Map<String, MapEntry> entries = Maps.newHashMapWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            String name = getKey(segment, i, keys[i]);
            entries.put(name, new MapEntry(segment, name, keys[i], values[i]));
        }
        return entries;
    }

    @Override
    RecordId getEntry(String key) {
        checkNotNull(key);

        if (size > 0) {
            int hash = key.hashCode();
            Segment segment = getSegment();

            int index = 0;
            while (index < size && getHash(segment, index) < hash) {
                index++;
            }
            while (index < size && getHash(segment, index) == hash) {
                if (key.equals(getKey(segment, index))) {
                    return getValue(segment, index);
                }
                index++;
            }
        }

        return null;
    }

    @Override
    Iterable<String> getKeys() {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return getKeyIterator();
            }
        };
    }

    @Override
    Iterable<MapEntry> getEntries() {
        return getMapEntries().values();
    }

    @Override
    boolean compare(MapRecord base, MapDiff diff) {
        if (base instanceof MapLeaf) {
            return compare((MapLeaf) base, diff);
        } else {
            return super.compare(base, diff);
        }
    }

    private boolean compare(MapLeaf before, MapDiff diff) {
        Segment bs = before.getSegment();
        int bi = 0;

        MapLeaf after = this;
        Segment as = after.getSegment();
        int ai = 0;

        while (ai < after.size) {
            int afterHash = after.getHash(as, ai);
            String afterKey = after.getKey(as, ai);
            RecordId afterValue = after.getValue(as, ai);

            while (bi < before.size
                    && (before.getHash(bs, bi) < afterHash
                        || (before.getHash(bs, bi) == afterHash
                            && before.getKey(bs, bi).compareTo(afterKey) < 0))) {
                if (!diff.entryDeleted(
                        before.getKey(bs, bi), before.getValue(bs, bi))) {
                    return false;
                }
                bi++;
            }

            if (bi < before.size
                    && before.getHash(bs, bi) == afterHash
                    && before.getKey(bs, bi).equals(afterKey)) {
                RecordId beforeValue = before.getValue(bs, bi);
                if (!afterValue.equals(beforeValue)
                        && !diff.entryChanged(afterKey, beforeValue, afterValue)) {
                    return false;
                }
                bi++;
            } else if (!diff.entryAdded(afterKey, afterValue)) {
                return false;
            }

            ai++;
        }

        while (bi < before.size) {
            if (!diff.entryDeleted(
                    before.getKey(bs, bi), before.getValue(bs, bi))) {
                return false;
            }
            bi++;
        }

        return true;
    }

    @Override
    public boolean compareAgainstEmptyMap(MapDiff diff) {
        Segment segment = getSegment();

        for (int i = 0; i < size; i++) {
            RecordId key = segment.readRecordId(getOffset(4 + size * 4, i));
            RecordId value = segment.readRecordId(getOffset(4 + size * 4, size + i));
            if (!diff.entryAdded(getKey(segment, i, key), value)) {
                return false;
            }
        }

        return true;
    }

    //-----------------------------------------------------------< private >--

    private Iterator<String> getKeyIterator() {
        return new Iterator<String>() {
            private final Segment segment = getSegment();
            private int index = 0;
            @Override
            public boolean hasNext() {
                return index < size;
            }
            @Override
            public String next() {
                int i = index++;
                if (i < size) {
                    return getKey(segment, i);
                } else {
                    throw new NoSuchElementException();
                }
            }
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private int getHash(Segment segment, int index) {
        return checkNotNull(segment).readInt(getOffset() + 4 + index * 4);
    }

    private synchronized String getKey(Segment segment, int index) {
        checkNotNull(segment);
        if (keys[index] == null) {
            int offset = getOffset(4 + size * 4, index);
            keys[index] = segment.readString(segment.readRecordId(offset));
        }
        return keys[index];
    }

    private synchronized String getKey(Segment segment, int index, RecordId id) {
        checkNotNull(segment);
        if (keys[index] == null) {
            keys[index] = segment.readString(id);
        }
        return keys[index];
    }

    private RecordId getValue(Segment segment, int index) {
        int offset = getOffset(4 + size * 4, size + index);
        return checkNotNull(segment).readRecordId(offset);
    }

}
