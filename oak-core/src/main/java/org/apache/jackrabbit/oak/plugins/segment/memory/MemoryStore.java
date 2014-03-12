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
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentIdFactory;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentWriter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Maps;

public class MemoryStore implements SegmentStore {

    private final SegmentIdFactory factory = new SegmentIdFactory();

    private final SegmentWriter writer = new SegmentWriter(this, factory);

    private SegmentNodeState head;

    private final ConcurrentMap<UUID, Segment> segments =
            Maps.newConcurrentMap();

    public MemoryStore(NodeState root) {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("root", root);

        SegmentWriter writer = getWriter();
        this.head = writer.writeNode(builder.getNodeState());
        writer.flush();
    }

    public MemoryStore() {
        this(EMPTY_NODE);
    }

    @Override
    public SegmentWriter getWriter() {
        return writer;
    }

    @Override
    public synchronized SegmentNodeState getHead() {
        return head;
    }

    @Override
    public synchronized boolean setHead(SegmentNodeState base, SegmentNodeState head) {
        if (this.head.getRecordId().equals(base.getRecordId())) {
            this.head = head;
            return true;
        } else {
            return false;
        }
    }

    @Override @Nonnull
    public Segment readSegment(UUID uuid) {
        Segment segment = writer.getCurrentSegment(uuid);
        if (segment == null) {
            segment = segments.get(uuid);
        }
        if (segment != null) {
            return segment;
        } else {
            throw new IllegalArgumentException("Segment not found: " + uuid);
        }
    }

    @Override
    public void writeSegment(
            UUID segmentId, byte[] data, int offset, int length) {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.put(data, offset, length);
        buffer.rewind();
        Segment segment = new Segment(this, factory, segmentId, buffer);
        if (segments.putIfAbsent(segmentId, segment) != null) {
            throw new IllegalStateException("Segment override: " + segmentId);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public Blob readBlob(String reference) {
        return null;
    }

}
