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
package org.apache.jackrabbit.oak.plugins.document.util;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.mongodb.BasicDBObject;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.bson.types.ObjectId;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Utility methods.
 */
public class Utils {

    /**
     * Approximate length of a Revision string.
     */
    private static final int REVISION_LENGTH =
            new Revision(System.currentTimeMillis(), 0, 0).toString().length();

    /**
     * Make sure the name string does not contain unnecessary baggage (shared
     * strings).
     * <p>
     * This is only needed for older versions of Java (before Java 7 update 6).
     * See also
     * http://mail.openjdk.java.net/pipermail/core-libs-dev/2012-May/010257.html
     * 
     * @param x the string
     * @return the new string
     */
    public static String unshareString(String x) {
        return new String(x);
    }
    
    public static int pathDepth(String path) {
        if (path.equals("/")) {
            return 0;
        }
        int depth = 0;
        for (int i = 0; i < path.length(); i++) {
            if (path.charAt(i) == '/') {
                depth++;
            }
        }
        return depth;
    }
    
    public static <K, V> Map<K, V> newMap() {
        return new TreeMap<K, V>();
    }

    public static <E> Set<E> newSet() {
        return new HashSet<E>();
    }

    @SuppressWarnings("unchecked")
    public static int estimateMemoryUsage(Map<?, Object> map) {
        if (map == null) {
            return 0;
        }
        int size = 0;

        for (Entry<?, Object> e : map.entrySet()) {
            if (e.getKey() instanceof Revision) {
                size += 32;
            } else {
                size += 48 + e.getKey().toString().length() * 2;
            }
            Object o = e.getValue();
            if (o instanceof String) {
                size += 48 + ((String) o).length() * 2;
            } else if (o instanceof Long) {
                size += 16;
            } else if (o instanceof Boolean) {
                size += 8;
            } else if (o instanceof Integer) {
                size += 8;
            } else if (o instanceof Map) {
                size += 8 + estimateMemoryUsage((Map<String, Object>) o);
            } else if (o == null) {
                // zero
            } else {
                throw new IllegalArgumentException("Can't estimate memory usage of " + o);
            }
        }

        if (map instanceof BasicDBObject) {
            // Based on empirical testing using JAMM
            size += 176;
            size += map.size() * 136;
        } else {
            // overhead for some other kind of map
            // TreeMap (80) + unmodifiable wrapper (32)
            size += 112; 
            // 64 bytes per entry
            size += map.size() * 64; 
        }
        return size;
    }

    /**
     * Generate a unique cluster id, similar to the machine id field in MongoDB ObjectId objects.
     * 
     * @return the unique machine id
     */
    public static int getUniqueClusterId() {
        ObjectId objId = new ObjectId();
        return objId._machine();
    }

    public static String escapePropertyName(String propertyName) {
        int len = propertyName.length();
        if (len == 0) {
            return "_";
        }
        // avoid creating a buffer if escaping is not needed
        StringBuilder buff = null;
        char c = propertyName.charAt(0);
        int i = 0;
        if (c == '_' || c == '$') {
            buff = new StringBuilder(len + 1);
            buff.append('_').append(c);
            i++;
        }
        for (; i < len; i++) {
            c = propertyName.charAt(i);
            char rep;
            switch (c) {
            case '.':
                rep = 'd';
                break;
            case '\\':
                rep = '\\';
                break;
            default:
                rep = 0;
            }
            if (rep != 0) {
                if (buff == null) {
                    buff = new StringBuilder(propertyName.substring(0, i));
                }
                buff.append('\\').append(rep);
            } else if (buff != null) {
                buff.append(c);
            }
        }
        return buff == null ? propertyName : buff.toString();
    }
    
    public static String unescapePropertyName(String key) {
        int len = key.length();
        if (key.startsWith("_")
                && (key.startsWith("__") || key.startsWith("_$") || len == 1)) {
            key = key.substring(1);
            len--;
        }
        // avoid creating a buffer if escaping is not needed
        StringBuilder buff = null;
        for (int i = 0; i < len; i++) {
            char c = key.charAt(i);
            if (c == '\\') {
                if (buff == null) {
                    buff = new StringBuilder(key.substring(0, i));
                }
                c = key.charAt(++i);
                if (c == '\\') {
                    // ok
                } else if (c == 'd') {
                    c = '.';
                }
                buff.append(c);
            } else if (buff != null) {
                buff.append(c);
            }
        }
        return buff == null ? key : buff.toString();
    }
    
    public static boolean isPropertyName(String key) {
        return !key.startsWith("_") || key.startsWith("__") || key.startsWith("_$");
    }

    public static String getIdFromPath(String path) {
        int depth = Utils.pathDepth(path);
        return depth + ":" + path;
    }
    
    public static String getPathFromId(String id) {
        int index = id.indexOf(':');
        return id.substring(index + 1);
    }

    public static String getPreviousIdFor(String id, Revision r) {
        StringBuilder sb = new StringBuilder(id.length() + REVISION_LENGTH + 3);
        int index = id.indexOf(':');
        int depth = 0;
        for (int i = 0; i < index; i++) {
            depth *= 10;
            depth += Character.digit(id.charAt(i), 10);
        }
        sb.append(depth + 1).append(":p");
        sb.append(id, index + 1, id.length());
        if (sb.charAt(sb.length() - 1) != '/') {
            sb.append('/');
        }
        r.toStringBuilder(sb);
        return sb.toString();
    }

    /**
     * Deep copy of a map that may contain map values.
     * 
     * @param source the source map
     * @param target the target map
     * @param <K> the type of the map key
     */
    public static <K> void deepCopyMap(Map<K, Object> source, Map<K, Object> target) {
        for (Entry<K, Object> e : source.entrySet()) {
            Object value = e.getValue();
            Comparator<? super K> comparator = null;
            if (value instanceof SortedMap) {
                @SuppressWarnings("unchecked")
                SortedMap<K, Object> map = (SortedMap<K, Object>) value;
                comparator = map.comparator();
            }
            if (value instanceof Map<?, ?>) {
                @SuppressWarnings("unchecked")
                Map<K, Object> old = (Map<K, Object>) value;
                Map<K, Object> c = new TreeMap<K, Object>(comparator);
                deepCopyMap(old, c);
                value = c;
            }
            target.put(e.getKey(), value);
        }
    }
    
    /**
     * Returns the lower key limit to retrieve the children of the given
     * <code>path</code>.
     *
     * @param path a path.
     * @return the lower key limit.
     */
    public static String getKeyLowerLimit(String path) {
        String from = PathUtils.concat(path, "a");
        from = getIdFromPath(from);
        from = from.substring(0, from.length() - 1);
        return from;
    }

    /**
     * Returns the upper key limit to retrieve the children of the given
     * <code>path</code>.
     *
     * @param path a path.
     * @return the upper key limit.
     */
    public static String getKeyUpperLimit(String path) {
        String to = PathUtils.concat(path, "z");
        to = getIdFromPath(to);
        to = to.substring(0, to.length() - 2) + "0";
        return to;
    }

    /**
     * Returns <code>true</code> if a revision tagged with the given revision
     * should be considered committed, <code>false</code> otherwise. Committed
     * revisions have a tag, which equals 'c' or starts with 'c-'.
     *
     * @param tag the tag (may be <code>null</code>).
     * @return <code>true</code> if committed; <code>false</code> otherwise.
     */
    public static boolean isCommitted(@Nullable String tag) {
        return tag != null && (tag.equals("c") || tag.startsWith("c-"));
    }

    /**
     * Resolve the commit revision for the given revision <code>rev</code> and
     * the associated commit tag.
     *
     * @param rev a revision.
     * @param tag the associated commit tag.
     * @return the actual commit revision for <code>rev</code>.
     */
    @Nonnull
    public static Revision resolveCommitRevision(@Nonnull Revision rev,
                                                 @Nonnull String tag) {
        return checkNotNull(tag).startsWith("c-") ?
                Revision.fromString(tag.substring(2)) : rev;
    }
}
