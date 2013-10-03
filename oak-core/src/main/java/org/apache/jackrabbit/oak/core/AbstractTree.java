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

package org.apache.jackrabbit.oak.core;

import java.util.Iterator;
import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Iterables.transform;
import static org.apache.jackrabbit.oak.api.Tree.Status.EXISTING;
import static org.apache.jackrabbit.oak.api.Tree.Status.MODIFIED;
import static org.apache.jackrabbit.oak.api.Tree.Status.NEW;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.isHidden;

/**
 * {@code AbstractTree} provides default implementations for most
 * read methods of {@code Tree}. Furthermore it handles the
 * {@link #setOrderableChildren(boolean) ordering} of child nodes
 * and hides internal items.
 */
public abstract class AbstractTree implements Tree {

    /**
     * Name of the internal property that contains the child order
     */
    public static final String OAK_CHILD_ORDER = ":childOrder";

    // TODO: make this configurable
    private static final String[] INTERNAL_NODE_NAMES = {IndexConstants.INDEX_CONTENT_NODE_NAME, MicroKernel.CONFLICT_NAME};

    /**
     * Name of this tree
     */
    protected String name;

    /**
     * The {@code NodeBuilder} for the underlying node state
     */
    protected NodeBuilder nodeBuilder;

    /**
     * Create a new {@code AbstractTree} instance
     * @param name  name of the tree
     * @param nodeBuilder  {@code NodeBuilder} for the underlying node state
     */
    protected AbstractTree(@Nonnull String name, @Nonnull NodeBuilder nodeBuilder) {
        this.name = checkNotNull(name);
        this.nodeBuilder = checkNotNull(nodeBuilder);
    }

    /**
     * @return  the underlying {@code NodeState} of this tree
     */
    @Nonnull
    public NodeState getNodeState() {
        return nodeBuilder.getNodeState();
    }

    /**
     * Factory method for creating child trees
     * @param name  name of the child tree
     * @return  child tree of this tree with the given {@code name}
     */
    @Nonnull
    protected abstract AbstractTree createChild(@Nonnull String name);

    /**
     * @return  {@code true} iff {@code getStatus() == Status.NEW}
     */
    protected abstract boolean isNew();

    /**
     * @return  {@code true} iff {@code getStatus() == Status.MODIFIED}
     */
    protected abstract boolean isModified();

    /**
     * @return {@code true} if this tree has orderable children;
     *         {@code false} otherwise.
     */
    protected boolean hasOrderableChildren() {
        return nodeBuilder.hasProperty(OAK_CHILD_ORDER);
    }

    /**
     * Returns the list of child names considering its ordering
     * when the {@link #OAK_CHILD_ORDER} property is set.
     *
     * @return the list of child names.
     */
    @Nonnull
    protected Iterable<String> getChildNames() {
        if (hasOrderableChildren()) {
            return new Iterable<String>() {
                @Override
                public Iterator<String> iterator() {
                    return new Iterator<String>() {
                        final PropertyState childOrder = nodeBuilder.getProperty(OAK_CHILD_ORDER);
                        int index;

                        @Override
                        public boolean hasNext() {
                            return index < childOrder.count();
                        }

                        @Override
                        public String next() {
                            return childOrder.getValue(STRING, index++);
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        } else {
            return nodeBuilder.getChildNodeNames();
        }
    }

    //------------------------------------------------------------< Tree >---

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isRoot() {
        return name.isEmpty();
    }

    @Override
    public String getPath() {
        if (isRoot()) {
            return "/";
        } else {
            StringBuilder sb = new StringBuilder();
            buildPath(sb);
            return sb.toString();
        }
    }

    private void buildPath(StringBuilder sb) {
        if (!isRoot()) {
            getParent().buildPath(sb);
            sb.append('/').append(name);
        }
    }

    @Override
    public Status getStatus() {
        if (isNew()) {
            return NEW;
        } else if (isModified()) {
            return MODIFIED;
        } else {
            return EXISTING;
        }
    }

    @Override
    public boolean exists() {
        return !isHidden(name) && nodeBuilder.exists();
    }

    @Override
    public abstract AbstractTree getParent();

    @Override
    public PropertyState getProperty(String name) {
        return !isHidden(checkNotNull(name))
            ? nodeBuilder.getProperty(name)
            : null;
    }

    @Override
    public boolean hasProperty(String name) {
        return (!isHidden(checkNotNull(name))) && nodeBuilder.hasProperty(name);
    }

    @Override
    public long getPropertyCount() {
        return size(getProperties());
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return filter(nodeBuilder.getProperties(),
            new Predicate<PropertyState>() {
                @Override
                public boolean apply(PropertyState propertyState) {
                    return !isHidden(propertyState.getName());
                }
            });
    }

    @Override
    public boolean hasChild(String name) {
        return createChild(checkNotNull(name)).exists();
    }

    @Override
    public long getChildrenCount(long max) {
        if (max + INTERNAL_NODE_NAMES.length < 0) {
            // avoid overflow (if max is near Long.MAX_VALUE)
            max = Long.MAX_VALUE;
        } else {
            // fetch a few more
            max += INTERNAL_NODE_NAMES.length;
        }
        long count = nodeBuilder.getChildNodeCount(max);
        for (String name : INTERNAL_NODE_NAMES) {
            if (nodeBuilder.hasChildNode(name)) {
                count--;
            }
        }
        return count;
    }

    @Override
    public Iterable<Tree> getChildren() {
        return transform(
                filter(getChildNames(), new Predicate<String>() {
                    @Override
                    public boolean apply(String name) {
                        return !isHidden(name);
                    }
                }),
                new Function<String, Tree>() {
                    @Override
                    public Tree apply(String name) {
                        return createChild(name);
                    }
                });
    }
}
