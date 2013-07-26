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
package org.apache.jackrabbit.oak.jcr.xml;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.UUID;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.ItemExistsException;
import javax.jcr.PathNotFoundException;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.version.VersionException;
import javax.jcr.version.VersionManager;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.IdentifierManager;
import org.apache.jackrabbit.oak.jcr.SessionContext;
import org.apache.jackrabbit.oak.jcr.security.AccessManager;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.DefinitionProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.EffectiveNodeTypeProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.xml.NodeInfo;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.spi.xml.ProtectedNodeImporter;
import org.apache.jackrabbit.oak.spi.xml.ProtectedPropertyImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

public class ImporterImpl implements Importer {
    private static final Logger log = LoggerFactory.getLogger(ImporterImpl.class);

    private final Tree importTargetTree;
    private final Tree ntTypesRoot;
    private final int uuidBehavior;

    private final String userID;
    private final AccessManager accessManager;
    private final IdentifierManager idManager;
    private final EffectiveNodeTypeProvider effectiveNodeTypeProvider;
    private final DefinitionProvider definitionProvider;

    private final Stack<Tree> parents;

    /**
     * helper object that keeps track of remapped uuid's and imported reference
     * properties that might need correcting depending on the uuid mappings
     */
    private final ReferenceChangeTracker refTracker;

    private final List<ProtectedItemImporter> pItemImporters = new ArrayList<ProtectedItemImporter>();

    /**
     * Currently active importer for protected nodes.
     */
    private ProtectedNodeImporter pnImporter;

    /**
     * Creates a new {@code SessionImporter} instance.
     */
    public ImporterImpl(String absPath,
                        SessionContext sessionContext,
                        Root root,
                        int uuidBehavior,
                        boolean isWorkspaceImport) throws RepositoryException {
        if (!PathUtils.isAbsolute(absPath)) {
            throw new RepositoryException("Not an absolute path: " + absPath);
        }
        String oakPath = sessionContext.getOakPathKeepIndex(absPath);
        if (oakPath == null) {
            throw new RepositoryException("Invalid name or path: " + absPath);
        }

        if (isWorkspaceImport && sessionContext.getSessionDelegate().hasPendingChanges()) {
            throw new RepositoryException("Pending changes on session. Cannot run workspace import.");
        }

        this.uuidBehavior = uuidBehavior;
        userID = sessionContext.getSessionDelegate().getAuthInfo().getUserID();

        importTargetTree = root.getTree(absPath);
        if (!importTargetTree.exists()) {
            throw new PathNotFoundException(absPath);
        }

        // TODO: review usage of write-root and object obtained from session-context (OAK-931)
        VersionManager vMgr = sessionContext.getVersionManager();
        if (!vMgr.isCheckedOut(absPath)) {
            throw new VersionException("Target node is checked in.");
        }
        if (sessionContext.getLockManager().isLocked(absPath)) {
            throw new LockException("Target node is locked.");
        }
        ntTypesRoot = root.getTree(NODE_TYPES_PATH);
        accessManager = sessionContext.getAccessManager();
        idManager = new IdentifierManager(root);
        effectiveNodeTypeProvider = sessionContext.getEffectiveNodeTypeProvider();
        definitionProvider = sessionContext.getDefinitionProvider();
        // TODO: end

        refTracker = new ReferenceChangeTracker();

        parents = new Stack<Tree>();
        parents.push(importTargetTree);

        pItemImporters.clear();
        for (ProtectedItemImporter importer : sessionContext.getProtectedItemImporters()) {
            if (importer.init(sessionContext.getSession(), root, sessionContext, isWorkspaceImport, uuidBehavior, refTracker)) {
                pItemImporters.add(importer);
            }
        }
    }

    private Tree createTree(@Nonnull Tree parent, @Nonnull NodeInfo nInfo, @CheckForNull String uuid) throws RepositoryException {
        String ntName = nInfo.getPrimaryTypeName();
        String value = (ntName != null) ? ntName : TreeUtil.getDefaultChildType(ntTypesRoot, parent, nInfo.getName());
        Tree child = TreeUtil.addChild(parent, nInfo.getName(), value, ntTypesRoot, userID);
        if (ntName != null) {
            accessManager.checkPermissions(child, child.getProperty(JcrConstants.JCR_PRIMARYTYPE), Permissions.NODE_TYPE_MANAGEMENT);
        }
        if (uuid != null) {
            child.setProperty(JcrConstants.JCR_UUID, uuid);
        }
        for (String mixin : nInfo.getMixinTypeNames()) {
            TreeUtil.addMixin(child, mixin, ntTypesRoot, userID);
        }
        return child;
    }

    private void createProperty(Tree tree, PropInfo pInfo, PropertyDefinition def) throws RepositoryException {
        List<Value> values = pInfo.getValues(pInfo.getTargetType(def));
        PropertyState propertyState;
        String name = pInfo.getName();
        int type = pInfo.getType();
        if (values.size() == 1 && !def.isMultiple()) {
            propertyState = PropertyStates.createProperty(name, values.get(0));
        } else {
            propertyState = PropertyStates.createProperty(name, values);
        }
        tree.setProperty(propertyState);
        if (type == PropertyType.REFERENCE || type == PropertyType.WEAKREFERENCE) {
            // store reference for later resolution
            refTracker.processedReference(new Reference(tree, name));
        }
    }

    private Tree resolveUUIDConflict(Tree parent,
                                     String conflictingId,
                                     NodeInfo nodeInfo) throws RepositoryException {
        Tree tree;
        Tree conflicting = idManager.getTree(conflictingId);
        if (conflicting != null && !conflicting.exists()) {
            conflicting = null;
        }

        if (uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW) {
            // create new with new uuid
            tree = createTree(parent, nodeInfo, UUID.randomUUID().toString());
            // remember uuid mapping
            if (isNodeType(tree, JcrConstants.MIX_REFERENCEABLE)) {
                refTracker.put(nodeInfo.getUUID(), TreeUtil.getString(tree, JcrConstants.JCR_UUID));
            }
        } else if (uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW) {
            // if conflicting node is shareable, then clone it
            String msg = "a node with uuid " + nodeInfo.getUUID() + " already exists!";
            log.debug(msg);
            throw new ItemExistsException(msg);
        } else if (uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING) {
            if (conflicting == null) {
                // since the conflicting node can't be read,
                // we can't remove it
                String msg = "node with uuid " + conflictingId + " cannot be removed";
                log.debug(msg);
                throw new RepositoryException(msg);
            }

            // make sure conflicting node is not importTargetNode or an ancestor thereof
            if (importTargetTree.getPath().startsWith(conflicting.getPath())) {
                String msg = "cannot remove ancestor node";
                log.debug(msg);
                throw new ConstraintViolationException(msg);
            }
            // remove conflicting
            conflicting.remove();
            // create new with given uuid
            tree = createTree(parent, nodeInfo, nodeInfo.getUUID());
        } else if (uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING) {
            if (conflicting == null) {
                // since the conflicting node can't be read,
                // we can't replace it
                String msg = "node with uuid " + conflictingId + " cannot be replaced";
                log.debug(msg);
                throw new RepositoryException(msg);
            }

            if (conflicting.isRoot()) {
                String msg = "root node cannot be replaced";
                log.debug(msg);
                throw new RepositoryException(msg);
            }
            // 'replace' current parent with parent of conflicting
            parent = conflicting.getParent();

            // replace child node
            //TODO ordering! (what happened to replace?)
            conflicting.remove();
            tree = createTree(parent, nodeInfo, nodeInfo.getUUID());
        } else {
            String msg = "unknown uuidBehavior: " + uuidBehavior;
            log.debug(msg);
            throw new RepositoryException(msg);
        }
        return tree;
    }

    //-----------------------------------------------------------< Importer >---

    @Override
    public void start() throws RepositoryException {
        // nop
    }

    @Override
    public void startNode(NodeInfo nodeInfo, List<PropInfo> propInfos)
            throws RepositoryException {
        Tree parent = parents.peek();
        Tree tree = null;
        String id = nodeInfo.getUUID();
        String nodeName = nodeInfo.getName();
        String ntName = nodeInfo.getPrimaryTypeName();

        if (parent == null) {
            log.debug("Skipping node: " + nodeName);
            // parent node was skipped, skip this child node too
            parents.push(null); // push null onto stack for skipped node
            // notify the p-i-importer
            if (pnImporter != null) {
                pnImporter.startChildInfo(nodeInfo, propInfos);
            }
            return;
        }

        NodeDefinition parentDef = getDefinition(parent);
        if (parentDef.isProtected()) {
            // skip protected node
            parents.push(null);
            log.debug("Skipping protected node: " + nodeName);

            if (pnImporter != null) {
                // pnImporter was already started (current nodeInfo is a sibling)
                // notify it about this child node.
                pnImporter.startChildInfo(nodeInfo, propInfos);
            } else {
                // no importer defined yet:
                // test if there is a ProtectedNodeImporter among the configured
                // importers that can handle this.
                // if there is one, notify the ProtectedNodeImporter about the
                // start of a item tree that is protected by this parent. If it
                // potentially is able to deal with it, notify it about the child node.
                for (ProtectedItemImporter pni : pItemImporters) {
                    if (pni instanceof ProtectedNodeImporter && ((ProtectedNodeImporter) pni).start(parent)) {
                        log.debug("Protected node -> delegated to ProtectedNodeImporter");
                        pnImporter = (ProtectedNodeImporter) pni;
                        pnImporter.startChildInfo(nodeInfo, propInfos);
                        break;
                    } /* else: p-i-Importer isn't able to deal with the protected tree.
                     try next. and if none can handle the passed parent the
                     tree below will be skipped */
                }
            }
            return;
        }

        if (parent.hasChild(nodeName)) {
            // a node with that name already exists...
            Tree existing = parent.getChild(nodeName);
            NodeDefinition def = getDefinition(existing);
            if (!def.allowsSameNameSiblings()) {
                // existing doesn't allow same-name siblings,
                // check for potential conflicts
                if (def.isProtected() && isNodeType(existing, ntName)) {
                    /*
                     use the existing node as parent for the possible subsequent
                     import of a protected tree, that the protected node importer
                     may or may not be able to deal with.
                     -> upon the next 'startNode' the check for the parent being
                        protected will notify the protected node importer.
                     -> if the importer is able to deal with that node it needs
                        to care of the complete subtree until it is notified
                        during the 'endNode' call.
                     -> if the import can't deal with that node or if that node
                        is the a leaf in the tree to be imported 'end' will
                        not have an effect on the importer, that was never started.
                    */
                    log.debug("Skipping protected node: " + existing);
                    parents.push(existing);
                    return;
                }
                if (def.isAutoCreated() && isNodeType(existing, ntName)) {
                    // this node has already been auto-created, no need to create it
                    tree = existing;
                } else {
                    // edge case: colliding node does have same uuid
                    // (see http://issues.apache.org/jira/browse/JCR-1128)
                    String existingIdentifier = IdentifierManager.getIdentifier(existing);
                    if (!(existingIdentifier.equals(id)
                            && (uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING
                            || uuidBehavior == ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING))) {
                        throw new ItemExistsException(
                                "Node with the same UUID exists:" + existing);
                    }
                    // fall through
                }
            }
        }

        if (tree == null) {
            // create node
            if (id == null) {
                // no potential uuid conflict, always add new node
                tree = createTree(parent, nodeInfo, id);
            } else {
                Tree conflicting = idManager.getTree(id);
                if (conflicting != null && conflicting.exists()) {
                    // resolve uuid conflict
                    tree = resolveUUIDConflict(parent, id, nodeInfo);
                    if (tree == null) {
                        // no new node has been created, so skip this node
                        parents.push(null); // push null onto stack for skipped node
                        log.debug("Skipping existing node " + nodeInfo.getName());
                        return;
                    }
                } else {
                    // create new with given uuid
                    tree = createTree(parent, nodeInfo, id);
                }
            }
        }

        // process properties
        for (PropInfo pi : propInfos) {
            // find applicable definition
            //TODO find better heuristics?
            PropertyDefinition def = pi.getPropertyDef(effectiveNodeTypeProvider.getEffectiveNodeType(tree));

            if (def.isProtected()) {
                // skip protected property
                log.debug("Skipping protected property " + pi.getName());

                // notify the ProtectedPropertyImporter.
                for (ProtectedItemImporter ppi : pItemImporters) {
                    if (ppi instanceof ProtectedPropertyImporter
                            && ((ProtectedPropertyImporter) ppi).handlePropInfo(tree, pi, def)) {
                        log.debug("Protected property -> delegated to ProtectedPropertyImporter");
                        break;
                    } /* else: p-i-Importer isn't able to deal with this property.
                             try next pp-importer */

                }
            } else {
                // regular property -> create the property
                createProperty(tree, pi, def);
            }
        }

        parents.push(tree);
    }


    @Override
    public void endNode(NodeInfo nodeInfo) throws RepositoryException {
        Tree parent = parents.pop();
        if (parent == null) {
            if (pnImporter != null) {
                pnImporter.endChildInfo();
            }
        } else if (getDefinition(parent).isProtected()) {
            if (pnImporter != null) {
                pnImporter.end(parent);
                // and reset the pnImporter field waiting for the next protected
                // parent -> selecting again from available importers
                pnImporter = null;
            }
        }
    }

    @Override
    public void end() throws RepositoryException {
        /**
         * adjust references that refer to uuids which have been mapped to
         * newly generated uuids on import
         */
        // 1. let protected property/node importers handle protected ref-properties
        //    and (protected) properties underneath a protected parent node.
        for (ProtectedItemImporter ppi : pItemImporters) {
            ppi.processReferences();
        }

        // 2. regular non-protected properties.
        Iterator<Object> iter = refTracker.getProcessedReferences();
        while (iter.hasNext()) {
            Object ref = iter.next();
            if (!(ref instanceof Reference)) {
                continue;
            }

            Reference reference = (Reference) ref;
            if (reference.isMultiple()) {
                Iterable<String> values = reference.property.getValue(Type.STRINGS);
                List<String> newValues = Lists.newArrayList();
                for (String original : values) {
                    String adjusted = refTracker.get(original);
                    if (adjusted != null) {
                        newValues.add(adjusted);
                    } else {
                        // reference doesn't need adjusting, just copy old value
                        newValues.add(original);
                    }
                }
                reference.setProperty(newValues);
            } else {
                String original = reference.property.getValue(Type.STRING);
                String adjusted = refTracker.get(original);
                if (adjusted != null) {
                    reference.setProperty(adjusted);
                }
            }
        }
        refTracker.clear();
    }

    private boolean isNodeType(Tree tree, String ntName) throws RepositoryException {
        return effectiveNodeTypeProvider.isNodeType(tree, ntName);
    }

    private NodeDefinition getDefinition(Tree tree) throws RepositoryException {
        if (tree.isRoot()) {
            return definitionProvider.getRootDefinition();
        } else {
            return definitionProvider.getDefinition(tree.getParent(), tree);
        }
    }

    private class Reference {

        private final Tree tree;
        private final PropertyState property;

        private Reference(Tree tree, String propertyName) {
            this.tree = tree;
            this.property = tree.getProperty(propertyName);
        }

        private boolean isMultiple() {
            return property.isArray();
        }

        private void setProperty(String newValue) {
            PropertyState prop = PropertyStates.createProperty(property.getName(), newValue, property.getType().tag());
            tree.setProperty(prop);
        }

        private void setProperty(Iterable<String> newValues) {
            PropertyState prop = PropertyStates.createProperty(property.getName(), newValues, property.getType());
            tree.setProperty(prop);
        }
    }
}
