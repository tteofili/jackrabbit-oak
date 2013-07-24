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
package org.apache.jackrabbit.oak.security.authorization;

import java.util.Map;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code AccessControlValidatorProvider} aimed to provide a root validator
 * that makes sure access control related content modifications (adding, modifying
 * and removing access control policies) are valid according to the
 * constraints defined by this access control implementation.
 */
class AccessControlValidatorProvider extends ValidatorProvider {

    private static final Logger log = LoggerFactory.getLogger(AccessControlValidatorProvider.class);

    private final SecurityProvider securityProvider;

    AccessControlValidatorProvider(@Nonnull SecurityProvider securityProvider) {
        this.securityProvider = securityProvider;
    }

    //--------------------------------------------------< ValidatorProvider >---
    @Nonnull
    @Override
    public Validator getRootValidator(NodeState before, NodeState after) {
        Tree rootBefore = new ImmutableTree(before);
        Tree rootAfter = new ImmutableTree(after);

        RestrictionProvider restrictionProvider = getConfig(AccessControlConfiguration.class).getRestrictionProvider();

        Root root = new ImmutableRoot(before);
        Map<String, Privilege> privileges = getPrivileges(root);
        PrivilegeBitsProvider privilegeBitsProvider = new PrivilegeBitsProvider(root);
        ReadOnlyNodeTypeManager ntMgr = ReadOnlyNodeTypeManager.getInstance(before);

        return new AccessControlValidator(rootBefore, rootAfter, privileges, privilegeBitsProvider, restrictionProvider, ntMgr);
    }

    private Map<String, Privilege> getPrivileges(Root root) {
        PrivilegeManager pMgr = getConfig(PrivilegeConfiguration.class).getPrivilegeManager(root, NamePathMapper.DEFAULT);
        ImmutableMap.Builder privileges = ImmutableMap.builder();
        try {
            for (Privilege privilege : pMgr.getRegisteredPrivileges()) {
                privileges.put(privilege.getName(), privilege);
            }
        } catch (RepositoryException e) {
            log.error("Unexpected error: failed to read privileges.", e);
        }
        return privileges.build();
    }

    private <T> T getConfig(Class<T> configClass) {
        return securityProvider.getConfiguration(configClass);
    }
}
