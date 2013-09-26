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
package org.apache.jackrabbit.oak.plugins.segment.memory;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.apache.jackrabbit.oak.plugins.segment.Journal;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentWriter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MemoryStore implements SegmentStore {

    private final Map<String, Journal> journals = Maps.newHashMap();

    private final ConcurrentMap<UUID, Segment> segments =
            Maps.newConcurrentMap();

    private final SegmentWriter writer = new SegmentWriter(this);

    public MemoryStore(NodeState root) {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("root", root);
        journals.put("root", new MemoryJournal(this, builder.getNodeState()));
    }

    public MemoryStore() {
        this(EMPTY_NODE);
    }

    @Override
    public SegmentWriter getWriter() {
        return writer;
    }

    @Override
    public void close() {
    }

    @Override
    public synchronized Journal getJournal(final String name) {
        Journal journal = journals.get(name);
        if (journal == null) {
            journal = new MemoryJournal(this, "root");
            journals.put(name, journal);
        }
        return journal;
    }

    @Override
    public Segment readSegment(UUID id) {
        Segment segment = segments.get(id);
        if (segment != null) {
            return segment;
        } else {
            throw new IllegalArgumentException("Segment not found: " + id);
        }
    }

    @Override
    public void writeSegment(
            UUID segmentId, byte[] data, int offset, int length,
            List<UUID> referencedSegmentIds) {
        byte[] buffer = new byte[length];
        System.arraycopy(data, offset, buffer, 0, length);
        Segment segment = new Segment(
                this, segmentId, ByteBuffer.wrap(buffer),
                Lists.newArrayList(referencedSegmentIds));
        if (segments.putIfAbsent(segment.getSegmentId(), segment) != null) {
            throw new IllegalStateException(
                    "Segment override: " + segment.getSegmentId());
        }
    }

    @Override
    public void deleteSegment(UUID segmentId) {
        if (segments.remove(segmentId) == null) {
            throw new IllegalStateException("Missing segment: " + segmentId);
        }
    }

}
