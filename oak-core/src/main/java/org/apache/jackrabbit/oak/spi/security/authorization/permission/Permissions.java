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
package org.apache.jackrabbit.oak.spi.security.authorization.permission;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Session;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.TreeLocation;

/**
 * Provides constants for permissions used in the OAK access evaluation as well
 * as permission related utility methods.
 */
public final class Permissions {

    private Permissions() {
    }

    public static final long NO_PERMISSION = 0;

    /**
     * @since OAK 1.0
     */
    public static final long READ_NODE = 1;

    /**
     * @since OAK 1.0
     */
    public static final long READ_PROPERTY = READ_NODE << 1;

    /**
     * @since OAK 1.0
     */
    public static final long ADD_PROPERTY = READ_PROPERTY << 1;

    /**
     * @since OAK 1.0
     */
    public static final long MODIFY_PROPERTY = ADD_PROPERTY << 1;

    public static final long REMOVE_PROPERTY = MODIFY_PROPERTY << 1;

    public static final long ADD_NODE = REMOVE_PROPERTY << 1;

    public static final long REMOVE_NODE = ADD_NODE << 1;

    public static final long READ_ACCESS_CONTROL = REMOVE_NODE << 1;

    public static final long MODIFY_ACCESS_CONTROL = READ_ACCESS_CONTROL << 1;

    public static final long NODE_TYPE_MANAGEMENT = MODIFY_ACCESS_CONTROL << 1;

    public static final long VERSION_MANAGEMENT = NODE_TYPE_MANAGEMENT << 1;

    public static final long LOCK_MANAGEMENT = VERSION_MANAGEMENT << 1;

    public static final long LIFECYCLE_MANAGEMENT = LOCK_MANAGEMENT << 1;

    public static final long RETENTION_MANAGEMENT = LIFECYCLE_MANAGEMENT << 1;

    public static final long MODIFY_CHILD_NODE_COLLECTION = RETENTION_MANAGEMENT << 1;

    public static final long NODE_TYPE_DEFINITION_MANAGEMENT = MODIFY_CHILD_NODE_COLLECTION << 1;

    public static final long NAMESPACE_MANAGEMENT = NODE_TYPE_DEFINITION_MANAGEMENT << 1;

    public static final long WORKSPACE_MANAGEMENT = NAMESPACE_MANAGEMENT << 1;

    public static final long PRIVILEGE_MANAGEMENT = WORKSPACE_MANAGEMENT << 1;

    /**
     * @since OAK 1.0
     */
    public static final long USER_MANAGEMENT = PRIVILEGE_MANAGEMENT << 1;

    public static final long READ = READ_NODE | READ_PROPERTY;

    /**
     * @since OAK 1.0
     */
    public static final long REMOVE = REMOVE_NODE | REMOVE_PROPERTY;

    public static final long SET_PROPERTY = ADD_PROPERTY | MODIFY_PROPERTY | REMOVE_PROPERTY;

    public static final long ALL = (READ
            | SET_PROPERTY
            | ADD_NODE | REMOVE_NODE
            | READ_ACCESS_CONTROL | MODIFY_ACCESS_CONTROL
            | NODE_TYPE_MANAGEMENT
            | VERSION_MANAGEMENT
            | LOCK_MANAGEMENT
            | LIFECYCLE_MANAGEMENT
            | RETENTION_MANAGEMENT
            | MODIFY_CHILD_NODE_COLLECTION
            | NODE_TYPE_DEFINITION_MANAGEMENT
            | NAMESPACE_MANAGEMENT
            | WORKSPACE_MANAGEMENT
            | PRIVILEGE_MANAGEMENT
            | USER_MANAGEMENT
    );

    public static final Map<Long, String> PERMISSION_NAMES = new LinkedHashMap<Long, String>();
    static {
        PERMISSION_NAMES.put(ALL, "ALL");
        PERMISSION_NAMES.put(READ, "READ");
        PERMISSION_NAMES.put(READ_NODE, "READ_NODE");
        PERMISSION_NAMES.put(READ_PROPERTY, "READ_PROPERTY");
        PERMISSION_NAMES.put(SET_PROPERTY, "SET_PROPERTY");
        PERMISSION_NAMES.put(ADD_PROPERTY, "ADD_PROPERTY");
        PERMISSION_NAMES.put(MODIFY_PROPERTY, "MODIFY_PROPERTY");
        PERMISSION_NAMES.put(REMOVE_PROPERTY, "REMOVE_PROPERTY");
        PERMISSION_NAMES.put(ADD_NODE, "ADD_NODE");
        PERMISSION_NAMES.put(REMOVE_NODE, "REMOVE_NODE");
        PERMISSION_NAMES.put(REMOVE, "REMOVE");
        PERMISSION_NAMES.put(MODIFY_CHILD_NODE_COLLECTION, "MODIFY_CHILD_NODE_COLLECTION");
        PERMISSION_NAMES.put(READ_ACCESS_CONTROL, "READ_ACCESS_CONTROL");
        PERMISSION_NAMES.put(MODIFY_ACCESS_CONTROL, "MODIFY_ACCESS_CONTROL");
        PERMISSION_NAMES.put(NODE_TYPE_MANAGEMENT, "NODE_TYPE_MANAGEMENT");
        PERMISSION_NAMES.put(VERSION_MANAGEMENT, "VERSION_MANAGEMENT");
        PERMISSION_NAMES.put(LOCK_MANAGEMENT, "LOCK_MANAGEMENT");
        PERMISSION_NAMES.put(LIFECYCLE_MANAGEMENT, "LIFECYCLE_MANAGEMENT");
        PERMISSION_NAMES.put(RETENTION_MANAGEMENT, "RETENTION_MANAGEMENT");
        PERMISSION_NAMES.put(NODE_TYPE_DEFINITION_MANAGEMENT, "NODE_TYPE_DEFINITION_MANAGEMENT");
        PERMISSION_NAMES.put(NAMESPACE_MANAGEMENT, "NAMESPACE_MANAGEMENT");
        PERMISSION_NAMES.put(WORKSPACE_MANAGEMENT, "WORKSPACE_MANAGEMENT");
        PERMISSION_NAMES.put(PRIVILEGE_MANAGEMENT, "PRIVILEGE_MANAGEMENT");
        PERMISSION_NAMES.put(USER_MANAGEMENT, "USER_MANAGEMENT");
    }

    /**
     * Returns names of the specified permissions.
     *
     * @param permissions The permissions for which the string representation
     * should be collected.
     * @return The names of the given permissions.
     */
    public static Set<String> getNames(long permissions) {
        if (PERMISSION_NAMES.containsKey(permissions)) {
            return ImmutableSet.of(PERMISSION_NAMES.get(permissions));
        } else {
            Set<String> names = new HashSet<String>();
            for (Map.Entry<Long, String> entry : PERMISSION_NAMES.entrySet()) {
                long key = entry.getKey();
                if ((permissions & key) == key) {
                    names.add(entry.getValue());
                }
            }
            return names;
        }
    }

    /**
     * Returns the names of the specified permissions separated by ','.
     *
     * @param permissions The permissions for which the string representation
     * should be collected.
     * @return The names of the given permissions separated by ',' such
     * that i can be passed to {@link Session#hasPermission(String, String)}
     * and {@link Session#checkPermission(String, String)}.
     */
    public static String getString(long permissions) {
        if (PERMISSION_NAMES.containsKey(permissions)) {
            return PERMISSION_NAMES.get(permissions);
        } else {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<Long, String> entry : PERMISSION_NAMES.entrySet()) {
                long key = entry.getKey();
                if ((permissions & key) == key) {
                    if (sb.length() != 0) {
                        sb.append(',');
                    }
                    sb.append(entry.getValue());
                }
            }
            return sb.toString();
        }
    }

    public static boolean isRepositoryPermission(long permission) {
        return permission == NAMESPACE_MANAGEMENT ||
                permission == NODE_TYPE_DEFINITION_MANAGEMENT ||
                permission == PRIVILEGE_MANAGEMENT ||
                permission == WORKSPACE_MANAGEMENT;
    }

    public static boolean includes(long permissions, long permissionsToTest) {
        return (permissions & permissionsToTest) == permissionsToTest;
    }

     /**
      * Returns those bits from {@code permissions} that are not present in
      * the {@code otherPermissions}, i.e. subtracts the other permissions
      * from permissions.<br>
      * If the specified {@code otherPermissions} do not intersect with
      * {@code permissions},  {@code permissions} are returned.<br>
      * If {@code permissions} is included in {@code otherPermissions},
      * {@link #NO_PERMISSION} is returned.
      *
      * @param permissions
      * @param otherPermissions
      * @return the differences of the 2 permissions or {@link #NO_PERMISSION}.
      */
    public static long diff(long permissions, long otherPermissions) {
        return permissions & ~otherPermissions;
    }

    /**
     * Returns the permissions that correspond the given jcr actions such as
     * specified in {@link Session#hasPermission(String, String)}. Note that
     * in addition to the regular JCR actions ({@link Session#ACTION_READ},
     * {@link Session#ACTION_ADD_NODE}, {@link Session#ACTION_REMOVE} and
     * {@link Session#ACTION_SET_PROPERTY}) the string may also contain
     * the names of all permissions defined by this class.
     *
     * @param jcrActions A comma separated string of JCR actions and permission
     * names.
     * @param location The tree location for which the permissions should be
     * calculated.
     * @param isAccessControlContent Flag to mark the given location as access
     * control content.
     * @return The permissions.
     * @throws IllegalArgumentException If the string contains unknown actions
     * or permission names.
     */
    public static long getPermissions(String jcrActions, TreeLocation location,
                                      boolean isAccessControlContent) {
        Set<String> actions = Sets.newHashSet(Arrays.asList(jcrActions.split(",")));
        long permissions = NO_PERMISSION;
        if (actions.remove(Session.ACTION_READ)) {
            if (isAccessControlContent) {
                permissions |= READ_ACCESS_CONTROL;
            } else if (!location.exists()) {
                permissions |= READ;
            } else if (location.getProperty() != null) {
                permissions |= READ_PROPERTY;
            } else {
                permissions |= READ_NODE;
            }
        }

        if (!actions.isEmpty()) {
            if (isAccessControlContent) {
                actions.removeAll(ImmutableSet.of(
                        Session.ACTION_ADD_NODE,
                        Session.ACTION_REMOVE,
                        Session.ACTION_SET_PROPERTY));
                permissions |= MODIFY_ACCESS_CONTROL;
            } else {
                if (actions.remove(Session.ACTION_ADD_NODE)) {
                    permissions |= ADD_NODE;
                }
                if (actions.remove(Session.ACTION_SET_PROPERTY)) {
                    if (location.getProperty() == null) {
                        permissions |= ADD_PROPERTY;
                    } else {
                        permissions |= MODIFY_PROPERTY;
                    }
                }
                if (actions.remove(Session.ACTION_REMOVE)) {
                    if (!location.exists()) {
                        permissions |= REMOVE;
                    } else if (location.getProperty() != null) {
                        permissions |= REMOVE_PROPERTY;
                    } else {
                        permissions |= REMOVE_NODE;
                    }
                }
            }
        }

        permissions |= getPermissions(actions);

        if (!actions.isEmpty()) {
            throw new IllegalArgumentException("Unknown actions: " + actions);
        }
        return permissions;
    }

    /**
     * Returns the permissions that correspond the given permission names.
     *
     * @param permissionNames A comma separated string of permission names.
     * @return The permissions.
     * @throws IllegalArgumentException If the string contains unknown actions
     * or permission names.
     */
    public static long getPermissions(@Nullable String permissionNames) {
        if (permissionNames == null || permissionNames.isEmpty()) {
            return NO_PERMISSION;
        } else {
            return getPermissions(Sets.newHashSet(Arrays.asList(permissionNames.split(","))));
        }
    }

    private static long getPermissions(@Nonnull Set<String> permissionNames) {
        long permissions = NO_PERMISSION;
        Iterator<Map.Entry<Long, String>> entryItr = PERMISSION_NAMES.entrySet().iterator();
        while (entryItr.hasNext() && !permissionNames.isEmpty()) {
            Map.Entry<Long,String> entry = entryItr.next();
            if (permissionNames.remove(entry.getValue())) {
                permissions |= entry.getKey();
            }
        }
        return permissions;
    }

    public static long getPermission(@Nullable String path, long defaultPermission) {
        long permission;
        if (NamespaceConstants.NAMESPACES_PATH.equals(path)) {
            permission = Permissions.NAMESPACE_MANAGEMENT;
        } else if (NodeTypeConstants.NODE_TYPES_PATH.equals(path)) {
            permission = Permissions.NODE_TYPE_DEFINITION_MANAGEMENT;
        } else if (VersionConstants.SYSTEM_PATHS.contains(path)) {
            permission = Permissions.VERSION_MANAGEMENT;
        } else if (PrivilegeConstants.PRIVILEGES_PATH.equals(path)) {
            permission = Permissions.PRIVILEGE_MANAGEMENT;
        } else {
            // FIXME: workspace-mgt (blocked by OAK-916)
            permission = defaultPermission;
        }
        return permission;
    }
}