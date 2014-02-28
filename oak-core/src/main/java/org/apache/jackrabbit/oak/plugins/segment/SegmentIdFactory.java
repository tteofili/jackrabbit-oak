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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Sets.newHashSetWithExpectedSize;

import java.lang.ref.WeakReference;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Factory for creating the UUID objects used to identify segments.
 * Weak references are used to keep track of all currently referenced
 * UUIDs, so that we can avoid garbage-collecting those segments.
 */
public class SegmentIdFactory {

    private static final long MSB_MASK = ~(0xfL << 12);

    private static final long VERSION = (0x4L << 12);

    private static final long LSB_MASK = ~(0xfL << 60);

    private static final long DATA = 0xAL << 60;

    private static final long BULK = 0xBL << 60;

    /**
     * Checks whether the given UUID identifies a data segment.
     *
     * @param id segment identifier
     * @return {@code true} for a data segment, {@code false} for bulk
     */
    public static boolean isDataSegmentId(UUID id) {
        return (id.getLeastSignificantBits() & ~LSB_MASK) == DATA; 
    }

    /**
     * Checks whether the given UUID identifies a bulk segment.
     *
     * @param id segment identifier
     * @return {@code true} for a bulk segment, {@code false} for data
     */
    public static boolean isBulkSegmentId(UUID id) {
        return (id.getLeastSignificantBits() & ~LSB_MASK) == BULK; 
    }

    /**
     * The random number source for generating new segment identifiers.
     */
    private final SecureRandom random = new SecureRandom();

    /**
     * Hash table of weak references to UUIDs that are currently referenced.
     * The size of the table is always a power of two, which optimizes the
     * {@link #expand()} operation. The table is indexed by the random UUID
     * bits, which guarantees uniform distribution of entries. Each table
     * entry is either {@code null} (when there are no matching UUIDs) or
     * a list of weak references to the matching UUIDs.
     */
    private final ArrayList<List<WeakReference<UUID>>> uuids = newArrayList(
            Collections.<List<WeakReference<UUID>>>nCopies(1024, null));

    /**
     * Returns all segment identifiers that are currently referenced in memory.
     *
     * @return referenced segment identifiers
     */
    synchronized Set<UUID> getReferencedSegmentIds() {
        Set<UUID> set = newHashSetWithExpectedSize(uuids.size());

        for (int i = 0; i < uuids.size(); i++) {
            List<WeakReference<UUID>> list = uuids.get(i);
            if (list != null) {
                Iterator<WeakReference<UUID>> iterator = list.iterator();
                while (iterator.hasNext()) {
                    UUID uuid = iterator.next().get();
                    if (uuid == null) {
                        iterator.remove();
                    } else {
                        set.add(uuid);
                    }
                }
                if (list.isEmpty()) {
                    uuids.set(i, null);
                }
            }
        }

        return set;
    }

    /**
     * 
     * @param msb
     * @param lsb
     * @return
     */
    synchronized UUID getSegmentId(long msb, long lsb) {
        int index = ((int) lsb) & (uuids.size() - 1);

        List<WeakReference<UUID>> list = uuids.get(index);
        if (list == null) {
            list = newLinkedList();
            uuids.set(index, list);
        }

        Iterator<WeakReference<UUID>> iterator = list.iterator();
        while (iterator.hasNext()) {
            UUID uuid = iterator.next().get();
            if (uuid == null) {
                iterator.remove();
            } else if (uuid.getMostSignificantBits() == msb
                    && uuid.getLeastSignificantBits() == lsb) {
                return uuid;
            }
        }

        UUID uuid = new UUID(msb, lsb);
        list.add(new WeakReference<UUID>(uuid));

        if (list.size() > 5) {
            expand();
        }

        return uuid;
    }

    UUID newDataSegmentId() {
        return newSegmentId(DATA);
    }

    UUID newBulkSegmentId() {
        return newSegmentId(BULK);
    }

    private UUID newSegmentId(long type) {
        long msb = (random.nextLong() & MSB_MASK) | VERSION;
        long lsb = (random.nextLong() & LSB_MASK) | type;
        return getSegmentId(msb, lsb);
    }

    private synchronized void expand() {
        int n = uuids.size();
        uuids.ensureCapacity(n * 2);
        for (int i = 0; i < n; i++) {
            List<WeakReference<UUID>> list = uuids.get(i);
            if (list == null) {
                uuids.add(null);
            } else {
                List<WeakReference<UUID>> newList = newLinkedList();

                Iterator<WeakReference<UUID>>iterator = list.iterator();
                while (iterator.hasNext()) {
                    WeakReference<UUID> reference = iterator.next();
                    UUID uuid = reference.get();
                    if (uuid == null) {
                        iterator.remove();
                    } else if ((uuid.getLeastSignificantBits() & n) != 0) {
                        iterator.remove();
                        newList.add(reference);
                    }
                }

                if (list.isEmpty()) {
                    uuids.set(i, null);
                }

                if (newList.isEmpty()) {
                    uuids.add(null);
                } else {
                    uuids.add(newList);
                }
            }
        }
    }

}
