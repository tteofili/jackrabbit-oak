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

import static com.google.common.base.Preconditions.checkState;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Memory cache for segments.
 */
public class SegmentCache {

    private static final int DEFAULT_MEMORY_CACHE_SIZE = 1 << 28; // 256MB

    private final int maximumSize;

    private int currentSize = 0;

    private final Map<UUID, Segment> segments =
        new LinkedHashMap<UUID, Segment>(1000, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Entry<UUID, Segment> eldest) {
                if (currentSize > maximumSize) {
                    currentSize -= eldest.getValue().size();
                    return true;
                } else {
                    return false;
                }
            }
        };

    private final Set<UUID> currentlyLoading = new HashSet<UUID>();

    public SegmentCache(int maximumSize) {
        this.maximumSize = maximumSize;
    }

    public SegmentCache() {
        this(DEFAULT_MEMORY_CACHE_SIZE);
    }

    public Segment getSegment(UUID segmentId, Callable<Segment> loader)
            throws Exception {
        synchronized (this) {
            Segment segment = segments.get(segmentId);

            while (segment == null && currentlyLoading.contains(segmentId)) {
                wait();
                segment = segments.get(segmentId);
            }

            if (segment != null) {
                return segment;
            } else {
                currentlyLoading.add(segmentId);
            }
        }

        Segment segment = loader.call();
        synchronized (this) {
            segments.put(segmentId, segment);
            currentSize += segment.size();
            currentlyLoading.remove(segmentId);
            notifyAll();
        }
        return segment;
    }

    public synchronized void addSegment(Segment segment) {
        checkState(!segments.containsKey(segment.getSegmentId()));
        checkState(!currentlyLoading.contains(segment.getSegmentId()));
        segments.put(segment.getSegmentId(), segment);
        currentSize += segment.size();
    }

    public synchronized void removeSegment(UUID segmentId)
            throws InterruptedException {
        while (currentlyLoading.contains(segmentId)) {
            wait();
        }
        Segment segment = segments.remove(segmentId);
        if (segment != null) {
            currentSize -= segment.size();
        }
    }

    public synchronized void clear() throws InterruptedException {
        while (!currentlyLoading.isEmpty()) {
            wait();
        }
        segments.clear();
        currentSize = 0;
    }

}
