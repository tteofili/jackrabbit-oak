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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;

final class AuthorizationContext implements Context, AccessControlConstants, PermissionConstants {

    private static final Context INSTANCE = new AuthorizationContext();

    private AuthorizationContext() {
    }

    static Context getInstance() {
        return INSTANCE;
    }

    //------------------------------------------------------------< Context >---
    @Override
    public boolean definesProperty(Tree parent, PropertyState property) {
        return definesTree(parent);
    }

    @Override
    public boolean definesContextRoot(@Nonnull Tree tree) {
        String name = tree.getName();
        return POLICY_NODE_NAMES.contains(name) || REP_PERMISSION_STORE.equals(name);
    }

    @Override
    public boolean definesTree(Tree tree) {
        String ntName = TreeUtil.getPrimaryTypeName(tree);
        return AC_NODETYPE_NAMES.contains(ntName) || PERMISSION_NODETYPE_NAMES.contains(ntName);
    }

    @Override
    public boolean definesLocation(TreeLocation location) {
        Tree tree = location.getTree();
        if (tree != null && location.exists()) {
            PropertyState p = location.getProperty();
            return (p == null) ? definesTree(tree) : definesProperty(tree, p);
        } else {
            String path = location.getPath();
            return definesPath(path, Text.getName(path));
        }
    }

    @Override
    public boolean definesPath(@Nonnull String treePath, @Nullable String propertyName) {
        boolean definesProperty = (propertyName != null) && ACE_PROPERTY_NAMES.contains(propertyName);
        return definesProperty || treePath.startsWith(PERMISSIONS_STORE_PATH) || containsPolicyName(treePath);
    }

    private static boolean containsPolicyName(@Nonnull String treePath) {
        for (String pName : POLICY_NODE_NAMES) {
            if (treePath.contains('/' + pName)) {
                return true;
            }
        }
        return false;
    }
}
