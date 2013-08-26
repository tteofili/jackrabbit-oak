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
package org.apache.jackrabbit.oak.plugins.mongomk;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.mongomk.util.Utils;

/**
 * Represents a node held in memory (in the cache for example).
 */
public class Node implements CacheValue {

    final String path;
    final Revision rev;
    final Map<String, String> properties = Utils.newMap();
    Revision lastRevision;
    
    Node(String path, Revision rev) {
        this.path = path;
        this.rev = rev;
    }
    
    void setProperty(String propertyName, String value) {
        if (value == null) {
            properties.remove(propertyName);
        } else {
            properties.put(propertyName, value);
        }
    }
    
    public String getProperty(String propertyName) {
        return properties.get(propertyName);
    }
    
    public Set<String> getPropertyNames() {
        return properties.keySet();
    }

    public void copyTo(Node newNode) {
        newNode.properties.putAll(properties);
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("path: ").append(path).append('\n');
        buff.append("rev: ").append(rev).append('\n');
        buff.append(properties);
        buff.append('\n');
        return buff.toString();
    }
    
    /**
     * Create an add node operation for this node.
     */
    UpdateOp asOperation(boolean isNew) {
        String id = Utils.getIdFromPath(path);
        UpdateOp op = new UpdateOp(id, isNew);
        op.set(Document.ID, id);
        NodeDocument.setModified(op, rev);
        op.setMapEntry(NodeDocument.DELETED, rev.toString(), "false");
        for (String p : properties.keySet()) {
            String key = Utils.escapePropertyName(p);
            op.setMapEntry(key, rev.toString(), properties.get(p));
        }
        return op;
    }

    public String getId() {
        return path + "@" + lastRevision;        
    }

    public void append(JsopWriter json, boolean includeId) {
        if (includeId) {
            json.key(":id").value(getId());
        }
        for (String p : properties.keySet()) {
            json.key(p).encodedValue(properties.get(p));
        }
    }

    public void setLastRevision(Revision lastRevision) {
        this.lastRevision = lastRevision;
    }

    public Revision getLastRevision() {
        return lastRevision;
    }
    
    @Override
    public int getMemory() {
        int size = 180 + path.length() * 2;
        for (Entry<String, String> e : properties.entrySet()) {
            size += 136 + e.getKey().length() * 2 + e.getValue().length() * 2;
        }
        return size;
    }

    /**
     * A list of children for a node.
     */
    static class Children implements CacheValue {

        final ArrayList<String> children = new ArrayList<String>();
        boolean hasMore = false;

        @Override
        public int getMemory() {
            int size = 114;
            for (String c : children) {
                size += c.length() * 2 + 56;
            }
            return size;
        }

        @Override
        public String toString() {
            return children.toString();
        }
        
    }

}
