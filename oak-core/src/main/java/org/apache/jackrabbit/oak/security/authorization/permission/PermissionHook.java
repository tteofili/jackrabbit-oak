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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.core.TreeTypeProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.commit.PostValidationHook;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Strings;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

/**
 * {@code CommitHook} implementation that processes any modification made to
 * access control content and updates persisted permission store associated
 * with access control related data stored in the repository.
 * <p>
 * The access control entries are grouped by principal and store below the store root based on the hash value of the
 * access controllable path. hash collisions are handled by adding subnodes accordingly.
 * <pre>
 *   /jcr:system/rep:permissionStore/crx.default
 *      /everyone
 *          /552423  [rep:PermissionStore]
 *              /0     [rep:Permissions]
 *              /1     [rep:Permissions]
 *              /c0     [rep:PermissionStore]
 *                  /0      [rep:Permissions]
 *                  /1      [rep:Permissions]
 *                  /2      [rep:Permissions]
 *              /c1     [rep:PermissionStore]
 *                  /0      [rep:Permissions]
 *                  /1      [rep:Permissions]
 *                  /2      [rep:Permissions]
 * </pre>
 */
public class PermissionHook implements PostValidationHook, AccessControlConstants, PermissionConstants {

    private static final Logger log = LoggerFactory.getLogger(PermissionHook.class);

    private final RestrictionProvider restrictionProvider;
    private final String workspaceName;

    private NodeBuilder permissionRoot;
    private ReadOnlyNodeTypeManager ntMgr;
    private PrivilegeBitsProvider bitsProvider;

    private Map<String, Acl> modified = new HashMap<String, Acl>();

    private Map<String, Acl> deleted = new HashMap<String, Acl>();

    public PermissionHook(String workspaceName, RestrictionProvider restrictionProvider) {
        this.workspaceName = workspaceName;
        this.restrictionProvider = restrictionProvider;
    }

    @Nonnull
    @Override
    public NodeState processCommit(final NodeState before, NodeState after) throws CommitFailedException {
        NodeBuilder rootAfter = after.builder();

        permissionRoot = getPermissionRoot(rootAfter);
        ntMgr = ReadOnlyNodeTypeManager.getInstance(before);
        bitsProvider = new PrivilegeBitsProvider(new ImmutableRoot(before));

        Diff diff = new Diff("");
        after.compareAgainstBaseState(before, diff);
        apply();
        return rootAfter.getNodeState();
    }

    private void apply() {
        for (Map.Entry<String, Acl> entry:deleted.entrySet()) {
            entry.getValue().remove();
        }
        for (Map.Entry<String, Acl> entry:modified.entrySet()) {
            entry.getValue().update();
        }
    }

    private boolean isACL(@Nonnull Tree tree) {
        return ntMgr.isNodeType(tree, NT_REP_ACL);
    }

    private boolean isACE(@Nonnull Tree tree) {
        return ntMgr.isNodeType(tree, NT_REP_ACE);
    }

    @Nonnull
    private NodeBuilder getPermissionRoot(NodeBuilder rootBuilder) {
        // permission root has been created during workspace initialization
        return rootBuilder.getChildNode(JCR_SYSTEM).getChildNode(REP_PERMISSION_STORE).getChildNode(workspaceName);
    }

    private static Tree getTree(String name, NodeState nodeState) {
        return new ImmutableTree(ImmutableTree.ParentProvider.UNSUPPORTED, name, nodeState, TreeTypeProvider.EMPTY);
    }

    private class Diff extends DefaultNodeStateDiff {

        private final String parentPath;

        private Diff(String parentPath) {
            this.parentPath = parentPath;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (NodeStateUtils.isHidden(name)) {
                // ignore hidden nodes
                return true;
            }
            String path = parentPath + "/" + name;
            Tree tree = getTree(name, after);
            if (isACL(tree)) {
                Acl acl = new Acl(parentPath, name, new AfterNode(path, after));
                modified.put(acl.accessControlledPath, acl);
            } else {
                after.compareAgainstBaseState(EMPTY_NODE, new Diff(path));
            }
            return true;
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            if (NodeStateUtils.isHidden(name)) {
                // ignore hidden nodes
                return true;
            }
            String path = parentPath + "/" + name;
            Tree beforeTree = getTree(name, before);
            Tree afterTree = getTree(name, after);
            if (isACL(beforeTree)) {
                if (isACL(afterTree)) {
                    Acl acl = new Acl(parentPath, name, new AfterNode(path, after));
                    modified.put(acl.accessControlledPath, acl);

                    // also consider to remove the ACL from removed entries of other principals
                    Acl beforeAcl = new Acl(parentPath, name, new BeforeNode(path, before));
                    beforeAcl.entries.keySet().removeAll(acl.entries.keySet());
                    if (!beforeAcl.entries.isEmpty()) {
                        deleted.put(parentPath, beforeAcl);
                    }

                } else {
                    Acl acl = new Acl(parentPath, name, new BeforeNode(path, before));
                    deleted.put(acl.accessControlledPath, acl);
                }
            } else if (isACL(afterTree)) {
                Acl acl = new Acl(parentPath, name, new AfterNode(path, after));
                modified.put(acl.accessControlledPath, acl);
            } else {
                after.compareAgainstBaseState(before, new Diff(path));
            }
            return true;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            if (NodeStateUtils.isHidden(name)) {
                // ignore hidden nodes
                return true;
            }
            String path = parentPath + "/" + name;
            Tree tree = getTree(name, before);
            if (isACL(tree)) {
                Acl acl = new Acl(parentPath, name, new BeforeNode(path, before));
                deleted.put(acl.accessControlledPath, acl);
            } else {
                EMPTY_NODE.compareAgainstBaseState(before, new Diff(path));
            }
            return true;
        }
    }

    private abstract static class Node {

        private final String path;

        private Node(String path) {
            this.path = path;
        }

        String getName() {
            return Text.getName(path);
        }

        abstract NodeState getNodeState();
    }

    private static final class BeforeNode extends Node {

        private final NodeState nodeState;

        BeforeNode(String parentPath, NodeState nodeState) {
            super(parentPath);
            this.nodeState = nodeState;
        }

        @Override
        NodeState getNodeState() {
            return nodeState;
        }
    }

    private static final class AfterNode extends Node {

        private final NodeBuilder builder;

        private AfterNode(String path, NodeState state) {
            super(path);
            this.builder = state.builder();
        }

        @Override
        NodeState getNodeState() {
            return builder.getNodeState();
        }
    }

    private class Acl {

        private final String accessControlledPath;

        private final String nodeName;

        private final Map<String, List<AcEntry>> entries = new HashMap<String, List<AcEntry>>();

        private Acl(String aclPath, String name, @Nonnull Node node) {
            if (name.equals(REP_REPO_POLICY)) {
                this.accessControlledPath = "";
            } else {
                this.accessControlledPath = aclPath.length() == 0 ? "/" : aclPath;
            }
            nodeName = PermissionUtil.getEntryName(accessControlledPath);
            int index = 0;
            Tree aclTree = getTree(node.getName(), node.getNodeState());
            for (Tree child : aclTree.getChildren()) {
                if (isACE(child)) {
                    AcEntry entry = new AcEntry(child, accessControlledPath, index);
                    List<AcEntry> list = entries.get(entry.principalName);
                    if (list == null) {
                        list = new ArrayList<AcEntry>();
                        entries.put(entry.principalName, list);
                    }
                    list.add(entry);
                    index++;
                }
            }
        }

        private void remove() {
            String msg = "Unable to remove permission entry";
            for (String principalName: entries.keySet()) {
                if (permissionRoot.hasChildNode(principalName)) {
                    NodeBuilder principalRoot = permissionRoot.getChildNode(principalName);

                    // find the ACL node that for this path and principal
                    NodeBuilder parent = principalRoot.getChildNode(nodeName);
                    if (!parent.exists()) {
                        continue;
                    }

                    // check if the node is the correct one
                    if (PermissionUtil.checkACLPath(parent, accessControlledPath)) {
                        // remove and reconnect child nodes
                        NodeBuilder newParent = null;
                        for (String childName : parent.getChildNodeNames()) {
                            if (childName.charAt(0) != 'c') {
                                continue;
                            }
                            NodeBuilder child = parent.getChildNode(childName);
                            if (newParent == null) {
                                newParent = child;
                            } else {
                                newParent.setChildNode(childName, child.getNodeState());
                                child.remove();
                            }
                        }
                        parent.remove();
                        if (newParent != null) {
                            principalRoot.setChildNode(nodeName, newParent.getNodeState());
                        }
                    } else {
                        // check if any of the child nodes match
                        for (String childName : parent.getChildNodeNames()) {
                            if (childName.charAt(0) != 'c') {
                                continue;
                            }
                            NodeBuilder child = parent.getChildNode(childName);
                            if (PermissionUtil.checkACLPath(child, accessControlledPath)) {
                                // remove child
                                child.remove();
                            }
                        }
                    }
                } else {
                    log.error("{} {}: Principal root missing.", msg, this);
                }
            }
        }

        private void update() {
            for (String principalName: entries.keySet()) {
                NodeBuilder principalRoot = permissionRoot.child(principalName);
                if (!principalRoot.hasProperty(JCR_PRIMARYTYPE)) {
                    principalRoot.setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE, Type.NAME);
                }
                NodeBuilder parent = principalRoot.child(nodeName);
                if (!parent.hasProperty(JCR_PRIMARYTYPE)) {
                    parent.setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE, Type.NAME);
                }

                // check if current parent already has the correct path
                if (parent.hasProperty(REP_ACCESS_CONTROLLED_PATH)) {
                    if (!PermissionUtil.checkACLPath(parent, accessControlledPath)) {
                        // hash collision, find a new child
                        NodeBuilder child = null;
                        int idx = 0;
                        for (String childName : parent.getChildNodeNames()) {
                            if (childName.charAt(0) != 'c') {
                                continue;
                            }
                            child = parent.getChildNode(childName);
                            if (PermissionUtil.checkACLPath(child, accessControlledPath)) {
                                break;
                            }
                            child = null;
                            idx++;
                        }
                        while (child == null) {
                            String name = "c" + String.valueOf(idx++);
                            child = parent.getChildNode(name);
                            if (child.exists()) {
                                child = null;
                            } else {
                                child = parent.child(name);
                                child.setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE, Type.NAME);
                            }
                        }
                        parent = child;
                        parent.setProperty(REP_ACCESS_CONTROLLED_PATH, accessControlledPath);
                    }
                } else {
                    // new parent
                    parent.setProperty(REP_ACCESS_CONTROLLED_PATH, accessControlledPath);
                }
                updateEntries(parent, entries.get(principalName));
            }
        }

        private void updateEntries(NodeBuilder parent, List<AcEntry> list) {
            // remove old entries
            for (String childName : parent.getChildNodeNames()) {
                if (childName.charAt(0) != 'c') {
                    parent.getChildNode(childName).remove();
                }
            }
            for (AcEntry ace: list) {
                NodeBuilder n = parent.child(String.valueOf(ace.index))
                        .setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSIONS, Type.NAME)
                        .setProperty(REP_IS_ALLOW, ace.isAllow)
                        .setProperty(REP_INDEX, ace.index)
                        .setProperty(ace.privilegeBits.asPropertyState(REP_PRIVILEGE_BITS));
                for (Restriction restriction : ace.restrictions) {
                    n.setProperty(restriction.getProperty());
                }
            }
        }
    }

    private class AcEntry {

        private final String accessControlledPath;
        private final String principalName;
        private final PrivilegeBits privilegeBits;
        private final boolean isAllow;
        private final Set<Restriction> restrictions;
        private final long index;
        private int hashCode = -1;

        private AcEntry(@Nonnull Tree aceTree, @Nonnull String accessControlledPath, long index) {
            this.accessControlledPath = accessControlledPath;
            principalName = Text.escapeIllegalJcrChars(checkNotNull(TreeUtil.getString(aceTree, REP_PRINCIPAL_NAME)));
            privilegeBits = bitsProvider.getBits(TreeUtil.getStrings(aceTree, REP_PRIVILEGES));
            isAllow = NT_REP_GRANT_ACE.equals(TreeUtil.getPrimaryTypeName(aceTree));
            restrictions = restrictionProvider.readRestrictions(Strings.emptyToNull(accessControlledPath), aceTree);
            this.index = index;
        }

        @Override
        public int hashCode() {
            if (hashCode == -1) {
                hashCode = Objects.hashCode(accessControlledPath, principalName, privilegeBits, isAllow, restrictions);
            }
            return hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof AcEntry) {
                AcEntry other = (AcEntry) o;
                return isAllow == other.isAllow
                        && privilegeBits.equals(other.privilegeBits)
                        && principalName.equals(other.principalName)
                        && accessControlledPath.equals(other.accessControlledPath)
                        && restrictions.equals(other.restrictions);
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(accessControlledPath);
            sb.append(';').append(principalName);
            sb.append(';').append(isAllow ? "allow" : "deny");
            sb.append(';').append(bitsProvider.getPrivilegeNames(privilegeBits));
            sb.append(';').append(restrictions);
            return sb.toString();
        }
    }
}
