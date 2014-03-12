/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.query.index;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * An index that traverses over a given subtree.
 */
public class TraversingIndex implements QueryIndex {

    @Override
    public Cursor query(Filter filter, NodeState rootState) {
        return Cursors.newTraversingCursor(filter, rootState);
    }

    @Override
    public double getCost(Filter filter, NodeState rootState) {
        if (filter.getFullTextConstraint() != null) {
            // not an appropriate index for full-text search
            return Double.POSITIVE_INFINITY;
        }
        if (filter.containsNativeConstraint()) {
            // not an appropriate index for native search
            return Double.POSITIVE_INFINITY;
        }
        if (filter.isAlwaysFalse()) {
            return 0;
        }
        double nodeCount = 10000000;
        // if the path is from a join, then the depth is not correct
        // (the path might be the root node), but that's OK
        String path = filter.getPath();
        PathRestriction restriction = filter.getPathRestriction();
        switch (restriction) {
        case NO_RESTRICTION:
            break;
        case EXACT:
            nodeCount = 1;
            break;
        case ALL_CHILDREN:
            if (!PathUtils.denotesRoot(path)) {
                for (int depth = PathUtils.getDepth(path); depth > 0; depth--) {
                    // estimate 10 child nodes per node
                    nodeCount /= 10;
                }
            }
            break;
        case PARENT:
            if (PathUtils.denotesRoot(path)) {
                nodeCount = 1;
            } else {
                nodeCount = PathUtils.getDepth(path);
            }
            break;
        case DIRECT_CHILDREN:
            // we expect 1 million child nodes in the worst case
            nodeCount = 1000000;
            break;
        default:
            throw new IllegalArgumentException("Unknown restriction: " + restriction);
        }        
        return nodeCount;
    }

    @Override
    public String getPlan(Filter filter, NodeState rootState) {
        return "traverse \"" + filter.getPathPlan() + '"';
    }

    @Override
    public String getIndexName() {
        return "traverse";
    }

}
