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

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.spi.commit.EmptyObserver;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class SegmentNodeStore extends AbstractNodeStore {

    static final String ROOT = "root";

    private final SegmentStore store;

    private final Journal journal;

    private final SegmentReader reader;

    private final Observer observer;

    private SegmentNodeState head;

    public SegmentNodeStore(SegmentStore store, String journal) {
        this.store = store;
        this.journal = store.getJournal(journal);
        this.reader = new SegmentReader(store);
        this.observer = EmptyObserver.INSTANCE;
        this.head = new SegmentNodeState(store, this.journal.getHead());
    }

    public SegmentNodeStore(SegmentStore store) {
        this(store, "root");
    }

    synchronized SegmentNodeState getHead() {
        NodeState before = head.getChildNode(ROOT);
        head = new SegmentNodeState(store, journal.getHead());
        NodeState after = head.getChildNode(ROOT);
        observer.contentChanged(before, after);
        return head;
    }

    boolean setHead(SegmentNodeState base, SegmentNodeState head) {
        return journal.setHead(base.getRecordId(), head.getRecordId());
    }

    @Override @Nonnull
    public synchronized NodeState getRoot() {
        return getHead().getChildNode(ROOT);
    }

    @Override @Nonnull
    public SegmentNodeStoreBranch branch() {
        return new SegmentNodeStoreBranch(
                this, new SegmentWriter(store), getHead());
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        SegmentWriter writer = new SegmentWriter(store);
        RecordId recordId = writer.writeStream(stream);
        writer.flush();
        return new SegmentBlob(reader, recordId);
    }

    @Override @Nonnull
    public synchronized String checkpoint(long lifetime) {
        checkArgument(lifetime > 0);
        // TODO: Guard the checkpoint from garbage collection
        return getHead().getRecordId().toString();
    }

    @Override @CheckForNull
    public synchronized NodeState retrieve(@Nonnull String checkpoint) {
        // TODO: Verify validity of the checkpoint
        RecordId id = RecordId.fromString(checkNotNull(checkpoint));
        return new SegmentNodeState(store, id).getChildNode(ROOT);
    }

}
