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
package org.apache.jackrabbit.oak.security.privilege;


import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.SystemRoot;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.PostCommitHook;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code RepositoryInitializer} that asserts the existence and node type of
 * the /jcr:system/jcr:privileges node that is used to store privilege definitions.
 * In addition it writes all built-in privilege definitions except jcr:all to
 * the repository.
 */
class PrivilegeInitializer implements RepositoryInitializer, PrivilegeConstants {

    private static final Logger log = LoggerFactory.getLogger(PrivilegeInitializer.class);

    @Override
    public NodeState initialize(NodeState state) {
        NodeBuilder root = state.builder();
        NodeBuilder system = root.child(JcrConstants.JCR_SYSTEM);
        system.setProperty(JcrConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_REP_SYSTEM, Type.NAME);

        if (!system.hasChildNode(REP_PRIVILEGES)) {
            NodeBuilder privileges = system.child(REP_PRIVILEGES);
            privileges.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_PRIVILEGES, Type.NAME);

            NodeStore store = new MemoryNodeStore();
            NodeStoreBranch branch = store.branch();
            try {
                branch.setRoot(root.getNodeState());
                branch.merge(EmptyHook.INSTANCE, PostCommitHook.EMPTY);
            } catch (CommitFailedException e) {
                log.error("Failed to initialize privilege content ", e);
                throw new RuntimeException(e);
            }

            try {
                new PrivilegeDefinitionWriter(new SystemRoot(store)).writeBuiltInDefinitions();
            } catch (RepositoryException e) {
                log.error("Failed to register built-in privileges", e);
                throw new RuntimeException(e);
            }
            return store.getRoot();
        }
        return root.getNodeState();
    }
}
