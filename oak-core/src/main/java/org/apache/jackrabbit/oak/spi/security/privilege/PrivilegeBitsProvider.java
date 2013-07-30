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
package org.apache.jackrabbit.oak.spi.security.privilege;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Reads and writes privilege definitions from and to the repository content
 * without applying any validation.
 */
public final class PrivilegeBitsProvider implements PrivilegeConstants {

    private final Map<PrivilegeBits, Set<String>> bitsToNames = new HashMap<PrivilegeBits, Set<String>>();

    private final Root root;

    public PrivilegeBitsProvider(Root root) {
        this.root = root;
    }

    /**
     * Returns the root tree for all privilege definitions stored in the content
     * repository.
     *
     * @return The privileges root.
     */
    @Nonnull
    public Tree getPrivilegesTree() {
        return PrivilegeUtil.getPrivilegesTree(root);
    }

    /**
     * @param privilegeNames
     * @return
     */
    @Nonnull
    public PrivilegeBits getBits(@Nonnull String... privilegeNames) {
        if (privilegeNames.length == 0) {
            return PrivilegeBits.EMPTY;
        }

        Tree privilegesTree = getPrivilegesTree();
        if (!privilegesTree.exists()) {
            return PrivilegeBits.EMPTY;
        }
        PrivilegeBits bits = PrivilegeBits.getInstance();
        for (String privilegeName : privilegeNames) {
            Tree defTree = privilegesTree.getChild(checkNotNull(privilegeName));
            if (defTree.exists()) {
                bits.add(PrivilegeBits.getInstance(defTree));
            }
        }
        return bits.unmodifiable();
    }

    /**
     * @param privilegeNames
     * @return
     */
    @Nonnull
    public PrivilegeBits getBits(@Nonnull Iterable<String> privilegeNames) {
        if (!privilegeNames.iterator().hasNext()) {
            return PrivilegeBits.EMPTY;
        }

        Tree privilegesTree = getPrivilegesTree();
        if (!privilegesTree.exists()) {
            return PrivilegeBits.EMPTY;
        }
        PrivilegeBits bits = PrivilegeBits.getInstance();
        for (String privilegeName : privilegeNames) {
            Tree defTree = privilegesTree.getChild(checkNotNull(privilegeName));
            if (defTree.exists()) {
                bits.add(PrivilegeBits.getInstance(defTree));
            }
        }
        return bits.unmodifiable();
    }

    /**
     * Resolve the given privilege bits to a set of privilege names.
     *
     * @param privilegeBits An instance of privilege bits.
     * @return The names of the registed privileges associated with the given
     *         bits. Any bits that don't have a corresponding privilege definition will
     *         be ignored.
     */
    @Nonnull
    public Set<String> getPrivilegeNames(PrivilegeBits privilegeBits) {
        if (privilegeBits == null || privilegeBits.isEmpty()) {
            return Collections.emptySet();
        } else if (bitsToNames.containsKey(privilegeBits)) {
            // matches all built-in aggregates and single built-in privileges
            return bitsToNames.get(privilegeBits);
        } else {
            Tree privilegesTree = getPrivilegesTree();
            if (!privilegesTree.exists()) {
                return Collections.emptySet();
            }

            if (bitsToNames.isEmpty()) {
                for (Tree child : privilegesTree.getChildren()) {
                    bitsToNames.put(PrivilegeBits.getInstance(child), Collections.singleton(child.getName()));
                }
            }

            Set<String> privilegeNames;
            if (bitsToNames.containsKey(privilegeBits)) {
                privilegeNames = bitsToNames.get(privilegeBits);
            } else {
                privilegeNames = new HashSet<String>();
                Set<String> aggregates = new HashSet<String>();
                for (Tree child : privilegesTree.getChildren()) {
                    PrivilegeBits bits = PrivilegeBits.getInstance(child);
                    if (privilegeBits.includes(bits)) {
                        privilegeNames.add(child.getName());
                        if (child.hasProperty(REP_AGGREGATES)) {
                            aggregates.addAll(PrivilegeUtil.readDefinition(child).getDeclaredAggregateNames());
                        }
                    }
                }
                privilegeNames.removeAll(aggregates);
                bitsToNames.put(privilegeBits.unmodifiable(), ImmutableSet.copyOf(privilegeNames));
            }
            return privilegeNames;
        }
    }
}
