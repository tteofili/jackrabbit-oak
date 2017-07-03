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
package org.apache.jackrabbit.oak.plugins.index.util;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.index.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Sets.newHashSet;
import static javax.jcr.PropertyType.TYPENAME_BINARY;
import static javax.jcr.PropertyType.TYPENAME_STRING;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.*;
import static org.apache.jackrabbit.oak.plugins.index.LuceneIndexConstants.*;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.GROUP_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.USER_PROPERTY_NAMES;

public class LuceneIndexHelper {

    public static final Set<String> JR_PROPERTY_INCLUDES = of(TYPENAME_STRING,
            TYPENAME_BINARY);

    /**
     * Nodes that represent content that shold not be tokenized (like UUIDs,
     * etc)
     * 
     */
    private final static Set<String> NOT_TOKENIZED = newHashSet(JCR_UUID);

    static {
        NOT_TOKENIZED.addAll(USER_PROPERTY_NAMES);
        NOT_TOKENIZED.addAll(GROUP_PROPERTY_NAMES);
    }

    private LuceneIndexHelper() {
    }

    public static NodeBuilder newLuceneIndexDefinition(
            @Nonnull NodeBuilder index, @Nonnull String name,
            @Nullable Set<String> propertyTypes) {
        return newLuceneIndexDefinition(index, name, propertyTypes, null, null, null);
    }

    public static NodeBuilder newLuceneIndexDefinition(
            @Nonnull NodeBuilder index, @Nonnull String name,
            @Nullable Set<String> propertyTypes,
            @Nullable Set<String> excludes, @Nullable String async) {
        return newLuceneIndexDefinition(index, name, propertyTypes, excludes,
                async, null);
    }

    public static NodeBuilder newLuceneIndexDefinition(
            @Nonnull NodeBuilder index, @Nonnull String name,
            @Nullable Set<String> propertyTypes,
            @Nullable Set<String> excludes, @Nullable String async,
            @Nullable Boolean stored) {
        if (index.hasChildNode(name)) {
            return index.child(name);
        }
        index = index.child(name);
        index.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, TYPE_LUCENE)
                .setProperty(REINDEX_PROPERTY_NAME, true);
        if (async != null) {
            index.setProperty(ASYNC_PROPERTY_NAME, async);
        }
        if (propertyTypes != null && !propertyTypes.isEmpty()) {
            index.setProperty(PropertyStates.createProperty(INCLUDE_PROPERTY_TYPES,
                    propertyTypes, STRINGS));
        }
        if (excludes != null && !excludes.isEmpty()) {
            index.setProperty(PropertyStates.createProperty(EXCLUDE_PROPERTY_NAMES, excludes,
                    STRINGS));
        }
        if (stored != null) {
            index.setProperty(PropertyStates.createProperty(EXPERIMENTAL_STORAGE, stored));
        }
        return index;
    }

    public static NodeBuilder newLuceneFileIndexDefinition(
            @Nonnull NodeBuilder index, @Nonnull String name,
            @Nullable Set<String> propertyTypes, @Nonnull String path) {
        return newLuceneFileIndexDefinition(index, name, propertyTypes, null,
                path, null);
    }

    public static NodeBuilder newLuceneFileIndexDefinition(
            @Nonnull NodeBuilder index, @Nonnull String name,
            @Nullable Set<String> propertyTypes,
            @Nullable Set<String> excludes, @Nonnull String path,
            @Nullable String async) {
        if (index.hasChildNode(name)) {
            return index.child(name);
        }
        index = index.child(name);
        index.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, TYPE_LUCENE)
                .setProperty(PERSISTENCE_NAME, PERSISTENCE_FILE)
                .setProperty(PERSISTENCE_PATH, path)
                .setProperty(REINDEX_PROPERTY_NAME, true);
        if (async != null) {
            index.setProperty(ASYNC_PROPERTY_NAME, async);
        }
        if (propertyTypes != null && !propertyTypes.isEmpty()) {
            index.setProperty(PropertyStates.createProperty(INCLUDE_PROPERTY_TYPES,
                    propertyTypes, STRINGS));
        }
        if (excludes != null && !excludes.isEmpty()) {
            index.setProperty(PropertyStates.createProperty(EXCLUDE_PROPERTY_NAMES, excludes,
                    STRINGS));
        }
        return index;
    }

    public static NodeBuilder newLucenePropertyIndexDefinition(
            @Nonnull NodeBuilder index, @Nonnull String name,
            @Nonnull Set<String> includes,
            @Nonnull String async) {
        checkArgument(!includes.isEmpty(), "Lucene property index " +
                "requires explicit list of property names to be indexed");

        index = index.child(name);
        index.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, TYPE_LUCENE)
                .setProperty(REINDEX_PROPERTY_NAME, true);
        index.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        index.setProperty(PropertyStates.createProperty(INCLUDE_PROPERTY_NAMES, includes, STRINGS));

        if (async != null) {
            index.setProperty(ASYNC_PROPERTY_NAME, async);
        }
        return index;
    }

    /**
     * Nodes that represent UUIDs and shold not be tokenized
     * 
     */
    public static boolean skipTokenization(String name) {
        return NOT_TOKENIZED.contains(name);
    }

    public static boolean isLuceneIndexNode(NodeState node){
        return TYPE_LUCENE.equals(node.getString(TYPE_PROPERTY_NAME));
    }
}
