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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.kernel.KernelNodeState;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.TimingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Iterables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DocumentNodeStoreTest {

    // OAK-1254
    @Test
    public void backgroundRead() throws Exception {
        final Semaphore semaphore = new Semaphore(1);
        DocumentStore docStore = new MemoryDocumentStore();
        DocumentStore testStore = new TimingDocumentStoreWrapper(docStore) {
            @Override
            public void invalidateCache() {
                super.invalidateCache();
                semaphore.acquireUninterruptibly();
                semaphore.release();
            }
        };
        final DocumentNodeStore store1 = new DocumentMK.Builder().setAsyncDelay(0)
                .setDocumentStore(testStore).setClusterId(1).getNodeStore();
        DocumentNodeStore store2 = new DocumentMK.Builder().setAsyncDelay(0)
                .setDocumentStore(docStore).setClusterId(2).getNodeStore();

        NodeBuilder builder = store2.getRoot().builder();
        builder.child("node2");
        store2.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // force update of _lastRevs
        store2.runBackgroundOperations();

        // at this point only node2 must not be visible
        assertFalse(store1.getRoot().hasChildNode("node2"));

        builder = store1.getRoot().builder();
        builder.child("node1");
        NodeState root =
                store1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        semaphore.acquireUninterruptibly();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                store1.runBackgroundOperations();
            }
        });
        t.start();
        // sleep until 'background thread' invalidated cache
        // and is waiting for semaphore
        while (!semaphore.hasQueuedThreads()) {
            Thread.sleep(10);
        }

        // must still not be visible at this state
        try {
            assertFalse(root.hasChildNode("node2"));
        } finally {
            semaphore.release();
        }
        t.join();
        // background operations completed
        root = store1.getRoot();
        // now node2 is visible
        assertTrue(root.hasChildNode("node2"));

        store1.dispose();
        store2.dispose();
    }

    @Test
    public void childNodeCache() throws Exception {
        DocumentNodeStore store = new DocumentMK.Builder().getNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        int max = (int) (KernelNodeState.MAX_CHILD_NAMES * 1.5);
        SortedSet<String> children = new TreeSet<String>();
        for (int i = 0; i < max; i++) {
            String name = "c" + i;
            children.add(name);
            builder.child(name);
        }
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        builder = store.getRoot().builder();
        String name = new ArrayList<String>(children).get(
                KernelNodeState.MAX_CHILD_NAMES / 2);
        builder.child(name).remove();
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        int numEntries = Iterables.size(store.getRoot().getChildNodeEntries());
        assertEquals(max - 1, numEntries);
        store.dispose();
    }

    @Test
    public void childNodeEntries() throws Exception {
        final AtomicInteger counter = new AtomicInteger();
        DocumentStore docStore = new MemoryDocumentStore() {
            @Nonnull
            @Override
            public <T extends Document> List<T> query(Collection<T> collection,
                                                      String fromKey,
                                                      String toKey,
                                                      int limit) {
                counter.incrementAndGet();
                return super.query(collection, fromKey, toKey, limit);
            }
        };
        DocumentNodeStore store = new DocumentMK.Builder()
                .setDocumentStore(docStore).getNodeStore();
        NodeBuilder root = store.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            root.child("node-" + i);
        }
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        counter.set(0);
        // the following should just make one call to DocumentStore.query()
        for (ChildNodeEntry e : store.getRoot().getChildNodeEntries()) {
            e.getNodeState();
        }
        assertEquals(1, counter.get());

        counter.set(0);
        // now the child node entries are cached and no call should happen
        for (ChildNodeEntry e : store.getRoot().getChildNodeEntries()) {
            e.getNodeState();
        }
        assertEquals(0, counter.get());

        store.dispose();
    }

    @Test
    public void rollback() throws Exception {
        final Map<Thread, Semaphore> locks = Collections.synchronizedMap(
                new HashMap<Thread, Semaphore>());
        final Semaphore created = new Semaphore(0);
        DocumentStore docStore = new MemoryDocumentStore() {
            @Override
            public <T extends Document> boolean create(Collection<T> collection,
                                                       List<UpdateOp> updateOps) {
                Semaphore semaphore = locks.get(Thread.currentThread());
                boolean result = super.create(collection, updateOps);
                if (semaphore != null) {
                    created.release();
                    semaphore.acquireUninterruptibly();
                }
                return result;
            }
        };
        final List<Exception> exceptions = new ArrayList<Exception>();
        final DocumentMK mk = new DocumentMK.Builder()
                .setDocumentStore(docStore).setAsyncDelay(0).open();
        final DocumentNodeStore store = mk.getNodeStore();
        final String head = mk.commit("/", "+\"foo\":{}+\"bar\":{}", null, null);
        Thread writer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Revision r = store.newRevision();
                    Commit c = new Commit(store, Revision.fromString(head), r);
                    c.addNode(new DocumentNodeState(store, "/foo/node", r));
                    c.addNode(new DocumentNodeState(store, "/bar/node", r));
                    c.apply();
                } catch (MicroKernelException e) {
                    exceptions.add(e);
                }
            }
        });
        final Semaphore s = new Semaphore(0);
        locks.put(writer, s);
        // will block in DocumentStore.create()
        writer.start();
        // wait for writer to create nodes
        created.acquireUninterruptibly();
        // commit will succeed and add collision marker to writer commit
        Revision r = store.newRevision();
        Commit c = new Commit(store, Revision.fromString(head), r);
        c.addNode(new DocumentNodeState(store, "/foo/node", r));
        c.addNode(new DocumentNodeState(store, "/bar/node", r));
        c.apply();
        // allow writer to continue
        s.release();
        writer.join();
        assertEquals("expected exception", 1, exceptions.size());

        String id = Utils.getIdFromPath("/foo/node");
        NodeDocument doc = docStore.find(Collection.NODES, id);
        assertNotNull("document with id " + id + " does not exist", doc);
        assertTrue(!doc.getLastRev().isEmpty());
        id = Utils.getIdFromPath("/bar/node");
        doc = docStore.find(Collection.NODES, id);
        assertNotNull("document with id " + id + " does not exist", doc);
        assertTrue(!doc.getLastRev().isEmpty());

        mk.dispose();
    }
}
