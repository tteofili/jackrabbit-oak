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
package org.apache.jackrabbit.oak.plugins.mongomk.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.plugins.mongomk.Collection;
import org.apache.jackrabbit.oak.plugins.mongomk.Document;
import org.apache.jackrabbit.oak.plugins.mongomk.DocumentStore;
import org.apache.jackrabbit.oak.plugins.mongomk.UpdateOp;

/**
 * A MicroKernel wrapper that can be used to log and also time MicroKernel
 * calls.
 */
public class TimingDocumentStoreWrapper implements DocumentStore {

    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("base.debug", "true"));
    private static final AtomicInteger NEXT_ID = new AtomicInteger();

    private final DocumentStore base;
    private final int id = NEXT_ID.getAndIncrement();

    private long startTime;
    private final Map<String, Count> counts = new HashMap<String, Count>();
    private long lastLogTime;
    private long totalLogTime;

    /**
     * A class that keeps track of timing data and call counts.
     */
    static class Count {
        public long count;
        public long max;
        public long total;
        public long paramSize;
        public long resultSize;

        void update(long time, int paramSize, int resultSize) {
            count++;
            if (time > max) {
                max = time;
            }
            total += time;
            this.paramSize += paramSize;
            this.resultSize += resultSize;
        }
    }

    public TimingDocumentStoreWrapper(DocumentStore base) {
        this.base = base;
        lastLogTime = now();
    }
    
    @Override
    @CheckForNull
    public <T extends Document> T find(Collection<T> collection, String key) {
        try {
            long start = now();
            T result = base.find(collection, key);
            updateAndLogTimes("find", start, 0, result.getMemory());
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    @CheckForNull
    public <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) {
        try {
            long start = now();
            T result = base.find(collection, key, maxCacheAge);
            updateAndLogTimes("find2", start, 0, result.getMemory());
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    @Nonnull
    public <T extends Document> List<T> query(Collection<T> collection,
                                                String fromKey,
                                                String toKey,
                                                int limit) {
        try {
            long start = now();
            List<T> result = base.query(collection, fromKey, toKey, limit);
            updateAndLogTimes("query", start, 0, size(result));
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    @Nonnull
    public <T extends Document> List<T> query(Collection<T> collection,
                                              String fromKey,
                                              String toKey,
                                              String indexedProperty,
                                              long startValue,
                                              int limit) {
        try {
            long start = now();
            List<T> result = base.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
            updateAndLogTimes("query2", start, 0, size(result));
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String key) {
        try {
            long start = now();
            base.remove(collection, key);
            updateAndLogTimes("remove", start, 0, 0);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) {
        try {
            long start = now();
            boolean result = base.create(collection, updateOps);
            updateAndLogTimes("create", start, 0, 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    @Nonnull
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update)
            throws MicroKernelException {
        try {
            long start = now();
            T result = base.createOrUpdate(collection, update);
            updateAndLogTimes("createOrUpdate", start, 0, result.getMemory());
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    @CheckForNull
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update)
            throws MicroKernelException {
        try {
            long start = now();
            T result = base.findAndUpdate(collection, update);
            updateAndLogTimes("findAndUpdate", start, 0, result.getMemory());
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public void invalidateCache() {
        try {
            long start = now();
            base.invalidateCache();
            updateAndLogTimes("invalidateCache", start, 0, 0);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String key) {
        try {
            long start = now();
            base.invalidateCache(collection, key);
            updateAndLogTimes("invalidateCache2", start, 0, 0);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public void dispose() {
        try {
            long start = now();
            base.dispose();
            updateAndLogTimes("dispose", start, 0, 0);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    @Override
    public <T extends Document> boolean isCached(Collection<T> collection, String key) {
        try {
            long start = now();
            boolean result = base.isCached(collection, key);
            updateAndLogTimes("isCached", start, 0, 0);
            return result;
        } catch (Exception e) {
            throw convert(e);
        }
    }

    private static RuntimeException convert(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        return new MicroKernelException("Unexpected exception: " + e.toString(), e);
    }

    private void log(String message) {
        if (DEBUG) {
            System.out.println("[" + id + "] " + message);
        }
    }
    
    private static <T extends Document> int size(List<T> list) {
        int result = 0;
        for (T doc : list) {
            result += doc.getMemory();
        }
        return result;
    }
    
    private static long now() {
        return System.currentTimeMillis();
    }

    private void updateAndLogTimes(String operation, long start, int paramSize, int resultSize) {
        long now = now();
        if (startTime == 0) {
            startTime = now;
        }
        Count c = counts.get(operation);
        if (c == null) {
            c = new Count();
            counts.put(operation, c);
        }
        c.update(now - start, paramSize, resultSize);
        long t = now - lastLogTime;
        if (t >= 2000) {
            totalLogTime += t;
            lastLogTime = now;
            long totalCount = 0, totalTime = 0;
            for (Count count : counts.values()) {
                totalCount += count.count;
                totalTime += count.total;
            }
            totalCount = Math.max(1, totalCount);
            totalTime = Math.max(1, totalTime);
            for (Entry<String, Count> e : counts.entrySet()) {
                c = e.getValue();
                long count = c.count;
                long total = c.total;
                long in = c.paramSize / 1024 / 1024;
                long out = c.resultSize / 1024 / 1024;
                if (count > 0) {
                    log(e.getKey() + 
                            " count " + count + 
                            " " + (100 * count / totalCount) + "%" +
                            " in " + in + " out " + out +
                            " time " + total +
                            " " + (100 * total / totalTime) + "%");
                }
            }
            log("all count " + totalCount + " time " + totalTime + " " + 
                    (100 * totalTime / totalLogTime) + "%");
            log("------");
        }
    }
    
}
