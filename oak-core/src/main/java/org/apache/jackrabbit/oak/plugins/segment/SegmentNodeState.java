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
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

public class SegmentNodeState extends AbstractNodeState {

    static boolean fastEquals(NodeState a, NodeState b) {
        return a instanceof SegmentNodeState
                && b instanceof SegmentNodeState
                && ((SegmentNodeState) a).recordId.equals(
                        ((SegmentNodeState) b).recordId);
    }

    private final SegmentStore store;

    private final RecordId recordId;

    private RecordId templateId = null;

    private Template template = null;

    public SegmentNodeState(SegmentStore store, RecordId id) {
        this.store = checkNotNull(store);
        this.recordId = checkNotNull(id);
    }

    public RecordId getRecordId() {
        return recordId;
    }

    RecordId getTemplateId() {
        getTemplate(); // force loading of the template
        return templateId;
    }

    synchronized Template getTemplate() {
        if (template == null) {
            Segment segment = store.readSegment(recordId.getSegmentId());
            templateId = segment.readRecordId(recordId.getOffset());
            template = segment.readTemplate(templateId);
        }
        return template;
    }

    MapRecord getChildNodeMap() {
        return getTemplate().getChildNodeMap(store, recordId);
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public long getPropertyCount() {
        return getTemplate().getPropertyCount();
    }

    @Override
    public boolean hasProperty(String name) {
        checkNotNull(name);
        return getTemplate().hasProperty(name);
    }

    @Override @CheckForNull
    public PropertyState getProperty(String name) {
        checkNotNull(name);
        return getTemplate().getProperty(name, store, recordId);
    }

    @Override @Nonnull
    public Iterable<PropertyState> getProperties() {
        return getTemplate().getProperties(store, recordId);
    }

    @Override
    public long getChildNodeCount(long max) {
        return getTemplate().getChildNodeCount(store, recordId);
    }

    @Override
    public boolean hasChildNode(String name) {
        checkArgument(!checkNotNull(name).isEmpty());
        return getTemplate().hasChildNode(name, store, recordId);
    }

    @Override @CheckForNull
    public NodeState getChildNode(String name) {
        // checkArgument(!checkNotNull(name).isEmpty()); // TODO
        return getTemplate().getChildNode(name, store, recordId);
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return getTemplate().getChildNodeNames(store, recordId);
    }

    @Override @Nonnull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return getTemplate().getChildNodeEntries(store, recordId);
    }

    @Override @Nonnull
    public NodeBuilder builder() {
        return new SegmentRootBuilder(this, store);
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (base == this) {
             return true; // no changes
        } else if (base == EMPTY_NODE || !base.exists()) { // special case
            return getTemplate().compareAgainstEmptyState(
                    store, recordId, diff);
        } else if (base instanceof SegmentNodeState) {
            SegmentNodeState that = (SegmentNodeState) base;
            return recordId.equals(that.recordId)
                || getTemplate().compareAgainstBaseState(
                        store, recordId, that.getTemplate(), that.recordId,
                        diff);
        } else {
            return super.compareAgainstBaseState(base, diff); // fallback
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof SegmentNodeState) {
            SegmentNodeState that = (SegmentNodeState) object;
            if (recordId.equals(that.recordId)) {
                return true;
            } else {
                Template template = getTemplate();
                return template.equals(that.getTemplate())
                        && template.compare(store, recordId, that.store, that.recordId);
            }
        } else {
            return super.equals(object);
        }
    }

}
