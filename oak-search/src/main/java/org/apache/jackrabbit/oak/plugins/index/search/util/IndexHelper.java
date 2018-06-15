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
package org.apache.jackrabbit.oak.plugins.index.search.util;

import java.util.Set;

import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Sets.newHashSet;
import static javax.jcr.PropertyType.TYPENAME_BINARY;
import static javax.jcr.PropertyType.TYPENAME_STRING;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.GROUP_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.USER_PROPERTY_NAMES;

/**
 * A helper class that helps decide what to (not) index.
 */
public class IndexHelper {

    public static final Set<String> JR_PROPERTY_INCLUDES = of(TYPENAME_STRING,
            TYPENAME_BINARY);

    /**
     * Nodes that represent content that should not be tokenized (like UUIDs,
     * etc)
     */
    private final static Set<String> NOT_TOKENIZED = newHashSet(JCR_UUID);

    static {
        NOT_TOKENIZED.addAll(USER_PROPERTY_NAMES);
        NOT_TOKENIZED.addAll(GROUP_PROPERTY_NAMES);
    }

    private IndexHelper() {
    }

    /**
     * Nodes that represent UUIDs and should not be tokenized
     */
    public static boolean skipTokenization(String name) {
        return NOT_TOKENIZED.contains(name);
    }

    public static boolean isIndexNodeOfType(NodeState node, String type){
        return type.equals(node.getString(TYPE_PROPERTY_NAME));
    }
}
