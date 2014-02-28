/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.cache;

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.cache.Cache;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.CachedNodeDocument;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;

/**
 * An OffHeap cache manages the cache value in an off heap storage.
 *
 * This interface is required to avoid direct dependency on DirectMemory
 * and Kryo classes
 */
public interface OffHeapCache extends Cache<CacheValue, NodeDocument> {

    Map<CacheValue, ? extends CachedNodeDocument> offHeapEntriesMap();

    CacheStats getCacheStats();

    @Nullable
    CachedNodeDocument getCachedDocument(String id);
}
