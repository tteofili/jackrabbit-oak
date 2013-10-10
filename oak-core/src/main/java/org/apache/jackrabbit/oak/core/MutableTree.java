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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.indexOf;
import static org.apache.jackrabbit.oak.api.Tree.Status.EXISTING;
import static org.apache.jackrabbit.oak.api.Tree.Status.MODIFIED;
import static org.apache.jackrabbit.oak.api.Tree.Status.NEW;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAbsolute;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.isHidden;

import java.util.Collections;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.AbstractRoot.Move;
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;

public class MutableTree extends AbstractTree {

    /**
     * Underlying {@code Root} of this {@code Tree} instance
     */
    private final AbstractRoot root;

    /**
     * Parent of this tree. Null for the root.
     */
    private MutableTree parent;

    /** Pointer into the list of pending moves */
    private Move pendingMoves;

    MutableTree(AbstractRoot root, NodeBuilder builder, Move pendingMoves) {
        super("", builder);
        this.root = checkNotNull(root);
        this.pendingMoves = checkNotNull(pendingMoves);
    }

    private MutableTree(AbstractRoot root, MutableTree parent, String name, Move pendingMoves) {
        super(name, parent.nodeBuilder.getChildNode(name));
        this.root = checkNotNull(root);
        this.parent = checkNotNull(parent);
        this.pendingMoves = checkNotNull(pendingMoves);
    }

    //-----------------------------------------------------< AbstractTree >---

    @Override
    protected MutableTree createChild(String name) {
        return new MutableTree(root, this, name, pendingMoves);
    }

    @Override
    protected boolean isNew() {
        return !getBase().exists();
    }

    @Override
    protected boolean isModified() {
        NodeState base = getBase();

        // child node removed?
        for (String name : base.getChildNodeNames()) {
            if (!nodeBuilder.hasChildNode(name)) {
                return true;
            }
        }

        // child node added?
        for (String name : nodeBuilder.getChildNodeNames()) {
            if (!base.hasChildNode(name)) {
                return true;
            }
        }

        // property removed?
        for (PropertyState p : base.getProperties()) {
            if (!nodeBuilder.hasProperty(p.getName())) {
                return true;
            }
        }

        // property added or modified?
        for (PropertyState p : nodeBuilder.getProperties()) {
            PropertyState q = base.getProperty(p.getName());
            if (q == null) {
                return true;
            }
            if (!p.equals(q)) {
                return true;
            }
        }

        return false;
    }

    //------------------------------------------------------------< Tree >---

    @Override
    public String getName() {
        beforeRead();
        return name;
    }

    @Override
    public String getPath() {
        beforeRead();
        return super.getPath();
    }

    @Override
    public Status getStatus() {
        beforeRead();
        return super.getStatus();
    }

    @Override
    public boolean exists() {
        beforeRead();
        return isVisible();
    }

    @Override
    public MutableTree getParent() {
        beforeRead();
        checkState(parent != null, "root tree does not have a parent");
        return parent;
    }

    @Override
    public PropertyState getProperty(String name) {
        beforeRead();
        return super.getProperty(name);
    }

    @Override
    public boolean hasProperty(String name) {
        beforeRead();
        return super.hasProperty(name);
    }

    @Override
    public long getPropertyCount() {
        beforeRead();
        return super.getPropertyCount();
    }

    @Override
    public Status getPropertyStatus(String name) {
        beforeRead();

        // make sure we don't expose information about a non-accessible property
        if (!hasProperty(name)) {
            return null;
        }

        // get status of this tree without checking for it's existence
        Status nodeStatus = super.getStatus();
        if (nodeStatus == NEW) {
            return (super.hasProperty(name)) ? NEW : null;
        }
        PropertyState head = super.getProperty(name);
        if (head == null) {
            return null;
        }

        PropertyState base = getSecureBase().getProperty(name);
        if (base == null) {
            return NEW;
        } else if (head.equals(base)) {
            return EXISTING;
        } else {
            return MODIFIED;
        }
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        beforeRead();
        return super.getProperties();
    }

    @Override
    public Tree getChild(String name) {
        beforeRead();
        return createChild(name);
    }

    @Override
    public boolean hasChild(String name) {
        beforeRead();
        return super.hasChild(name);
    }

    @Override
    public long getChildrenCount(long max) {
        beforeRead();
        return super.getChildrenCount(max);
    }

    @Override
    public Iterable<Tree> getChildren() {
        beforeRead();
        return super.getChildren();
    }

    @Override
    public boolean remove() {
        beforeWrite();
        if (parent != null && parent.hasChild(name)) {
            nodeBuilder.remove();
            if (parent.hasOrderableChildren()) {
                // FIXME (OAK-842) child order not updated when parent is not accessible
                parent.nodeBuilder.setProperty(
                        MemoryPropertyBuilder.copy(STRING, parent.nodeBuilder.getProperty(OAK_CHILD_ORDER))
                                .removeValue(name)
                                .getPropertyState()
                );
            }
            root.updated();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Tree addChild(String name) {
        beforeWrite();
        if (!super.hasChild(name)) {
            nodeBuilder.setChildNode(name);
            if (hasOrderableChildren()) {
                nodeBuilder.setProperty(
                        MemoryPropertyBuilder.copy(STRING, nodeBuilder.getProperty(OAK_CHILD_ORDER))
                                .addValue(name)
                                .getPropertyState());
            }
            root.updated();
        }
        return createChild(name);
    }

    @Override
    public void setOrderableChildren(boolean enable) {
        beforeWrite();
        if (enable) {
            ensureChildOrderProperty();
        } else {
            nodeBuilder.removeProperty(OAK_CHILD_ORDER);
        }
    }

    @Override
    public boolean orderBefore(final String name) {
        beforeWrite();
        if (parent == null) {
            // root does not have siblings
            return false;
        }
        if (name != null) {
            if (name.equals(this.name) || !parent.hasChild(name)) {
                // same node or no such sibling (not existing or not accessible)
                return false;
            }
        }
        // perform the reorder
        parent.ensureChildOrderProperty();
        // all siblings but not this one
        Iterable<String> siblings = filter(
                parent.getChildNames(),
                new Predicate<String>() {
                    @Override
                    public boolean apply(String name) {
                        return !MutableTree.this.name.equals(name);
                    }
                });
        // create head and tail
        Iterable<String> head;
        Iterable<String> tail;
        if (name == null) {
            head = siblings;
            tail = Collections.emptyList();
        } else {
            int idx = indexOf(siblings, new Predicate<String>() {
                @Override
                public boolean apply(String sibling) {
                    return name.equals(sibling);
                }
            });
            head = Iterables.limit(siblings, idx);
            tail = Iterables.skip(siblings, idx);
        }
        // concatenate head, this name and tail
        parent.nodeBuilder.setProperty(
                MultiStringPropertyState.stringProperty(
                        OAK_CHILD_ORDER, Iterables.concat(head, Collections.singleton(getName()), tail))
        );
        root.updated();
        return true;
    }

    @Override
    public void setProperty(PropertyState property) {
        beforeWrite();
        nodeBuilder.setProperty(property);
        root.updated();
    }

    @Override
    public <T> void setProperty(String name, T value) {
        beforeWrite();
        nodeBuilder.setProperty(name, value);
        root.updated();
    }

    @Override
    public <T> void setProperty(String name, T value, Type<T> type) {
        beforeWrite();
        nodeBuilder.setProperty(name, value, type);
        root.updated();
    }

    @Override
    public void removeProperty(String name) {
        beforeWrite();
        nodeBuilder.removeProperty(name);
        root.updated();
    }

    //-----------------------------------------------------------< Object >---

    @Override
    public String toString() {
        return getPathInternal() + ": " + getNodeState();
    }

    //---------------------------------------------------------< internal >---

    private NodeState getBase() {
        if (parent == null) {
            return root.getBaseState();
        } else {
            return parent.getBase().getChildNode(name);
        }
    }

    /**
     * Set the parent and name of this tree.
     * @param parent  parent of this tree
     * @param name  name of this tree
     */
    void setParentAndName(MutableTree parent, String name) {
        this.name = name;
        this.parent = parent;
    }

    /**
     * Move this tree to the parent at {@code destParent} with the new name
     * {@code newName}.
     * @param newParent new parent for this tree
     * @param newName   new name for this tree
     */
    boolean moveTo(MutableTree newParent, String newName) {
        name = newName;
        parent = newParent;
        return nodeBuilder.moveTo(newParent.nodeBuilder, newName);
    }

    /**
     * Copy this tree to the parent at {@code destParent} with the new name
     * {@code newName}.
     * @param newParent new parent for this tree
     * @param newName   new name for this tree
     */
    boolean copyTo(MutableTree newParent, String newName) {
        return nodeBuilder.copyTo(newParent.nodeBuilder, newName);
    }

    /**
     * Get a possibly non existing tree.
     * @param path the path to the tree
     * @return a {@link Tree} instance for the child at {@code path}.
     */
    @CheckForNull
    MutableTree getTree(@Nonnull String path) {
        checkArgument(isAbsolute(checkNotNull(path)));
        beforeRead();
        MutableTree child = this;
        for (String name : elements(path)) {
            child = new MutableTree(root, child, name, pendingMoves);
        }
        return child;
    }

    /**
     * Update the child order with children that have been removed or added.
     * Added children are appended to the end of the {@link #OAK_CHILD_ORDER}
     * property.
     */
    void updateChildOrder() {
        if (!hasOrderableChildren()) {
            return;
        }
        Set<String> names = Sets.newLinkedHashSet();
        for (String name : getChildNames()) {
            if (nodeBuilder.hasChildNode(name)) {
                names.add(name);
            }
        }
        for (String name : nodeBuilder.getChildNodeNames()) {
            names.add(name);
        }
        PropertyBuilder<String> builder = MemoryPropertyBuilder.array(
                STRING, OAK_CHILD_ORDER);
        builder.setValues(names);
        nodeBuilder.setProperty(builder.getPropertyState());
    }

    String getPathInternal() {
        if (parent == null) {
            return "/";
        } else {
            StringBuilder sb = new StringBuilder();
            buildPath(sb);
            return sb.toString();
        }
    }

    private void buildPath(StringBuilder sb) {
        if (parent != null) {
            parent.buildPath(sb);
            sb.append('/').append(name);
        }
    }

    //------------------------------------------------------------< private >---

    private void reconnect() {
        if (parent != null) {
            parent.reconnect();
            nodeBuilder = parent.nodeBuilder.getChildNode(name);
        }
    }

    /**
     * Verifies that this session is still alive and applies any pending
     * moves that might affect this node. This method needs to be called
     * at the beginning of all public read-only {@link Tree} methods to
     * guarantee a consistent view of the tree. See {@link #beforeWrite()}
     * for the equivalent method for write operations.
     *
     * @throws IllegalStateException if this session is closed
     */
    private void beforeRead() throws IllegalStateException {
        root.checkLive();
        if (applyPendingMoves()) {
            reconnect();
        }
    }

    /**
     * Like {@link #beforeRead()} but also checks that (after any pending
     * moves have been applied) the current node exists and is visible.
     * This method needs to be called at the beginning of all public
     * {@link Tree} methods that modify this node to guarantee a consistent
     * view of the tree and to throw an exception whenever there's an
     * attempt to modify a missing node.
     *
     * @throws IllegalStateException if this node does not exist or
     *                               if this session is closed
     */
    private void beforeWrite() throws IllegalStateException {
        beforeRead();
        if (!isVisible()) {
            throw new IllegalStateException("This tree does not exist");
        }
    }

    /**
     * Internal method for checking whether this node exists and is visible
     * (i.e. not hidden).
     *
     * @return {@true} if the node is visible, {@code false} if not
     */
    private boolean isVisible() {
        return !isHidden(name) && nodeBuilder.exists();
    }

    /**
     * The (possibly non-existent) node state this tree is based on.
     * @return the base node state of this tree
     */
    @Nonnull
    private NodeState getSecureBase() {
        if (parent == null) {
            return root.getSecureBase();
        } else {
            return parent.getSecureBase().getChildNode(name);
        }
    }

    private boolean applyPendingMoves() {
        boolean movesApplied = false;
        if (parent != null) {
            movesApplied = parent.applyPendingMoves();
        }
        Move old = pendingMoves;
        pendingMoves = pendingMoves.apply(this);
        if (pendingMoves != old) {
            movesApplied = true;
        }
        return movesApplied;
    }

    /**
     * Ensures that the {@link #OAK_CHILD_ORDER} exists. This method will create
     * the property if it doesn't exist and initialize the value with the names
     * of the children as returned by {@link NodeBuilder#getChildNodeNames()}.
     */
    private void ensureChildOrderProperty() {
        if (!nodeBuilder.hasProperty(OAK_CHILD_ORDER)) {
            nodeBuilder.setProperty(
                    MultiStringPropertyState.stringProperty(OAK_CHILD_ORDER, nodeBuilder.getChildNodeNames()));
        }
    }

}


