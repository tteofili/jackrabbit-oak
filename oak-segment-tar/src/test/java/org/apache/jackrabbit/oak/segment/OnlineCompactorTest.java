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
 *
 */

package org.apache.jackrabbit.oak.segment;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState.binaryPropertyFromBlob;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OnlineCompactorTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private FileStore fileStore;

    private SegmentNodeStore nodeStore;

    @Before
    public void setup() throws IOException, InvalidFileStoreVersionException {
        fileStore = fileStoreBuilder(folder.getRoot()).build();
        nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
    }

    @After
    public void tearDown() {
        fileStore.close();
    }

    @Test
    public void testCompact() throws Exception {
        OnlineCompactor compactor = createCompactor(fileStore, Suppliers.ofInstance(false));
        addTestContent(nodeStore);

        NodeState uncompacted = nodeStore.getRoot();
        SegmentNodeState compacted = compactor.compact(uncompacted);
        assertNotNull(compacted);
        assertFalse(uncompacted == compacted);
        assertEquals(uncompacted, compacted);
        assertEquals(1, compacted.getSegment().getGcGeneration());

        modifyTestContent(nodeStore);
        NodeState modified = nodeStore.getRoot();
        compacted = compactor.compact(uncompacted, modified, compacted);
        assertNotNull(compacted);
        assertFalse(modified == compacted);
        assertEquals(modified, compacted);
        assertEquals(1, compacted.getSegment().getGcGeneration());
    }

    @Test
    public void testExceedUpdateLimit() throws Exception {
        OnlineCompactor compactor = createCompactor(fileStore, Suppliers.ofInstance(false));
        addNodes(nodeStore, OnlineCompactor.UPDATE_LIMIT * 2 + 1);

        NodeState uncompacted = nodeStore.getRoot();
        SegmentNodeState compacted = compactor.compact(uncompacted);
        assertNotNull(compacted);
        assertFalse(uncompacted == compacted);
        assertEquals(uncompacted, compacted);
        assertEquals(1, compacted.getSegment().getGcGeneration());
    }

    @Test
    public void testCancel() throws IOException, CommitFailedException {
        OnlineCompactor compactor = createCompactor(fileStore, Suppliers.ofInstance(true));
        addTestContent(nodeStore);
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.setChildNode("cancel").setProperty("cancel", "cancel");
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertNull(compactor.compact(nodeStore.getRoot()));
    }

    @Nonnull
    private static OnlineCompactor createCompactor(FileStore fileStore, Supplier<Boolean> cancel) {
        SegmentWriter writer = defaultSegmentWriterBuilder("c").withGeneration(1).build(fileStore);
        return new OnlineCompactor(fileStore.getReader(), writer, fileStore.getBlobStore(), cancel);
    }

    private static void addNodes(SegmentNodeStore nodeStore, int count)
    throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        for (int k = 0; k < count; k++) {
            builder.setChildNode("n-" + k);
        }
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static void addTestContent(NodeStore nodeStore) throws CommitFailedException, IOException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.setChildNode("a").setChildNode("aa").setProperty("p", 42);
        builder.setChildNode("b").setProperty("bin", createBlob(nodeStore, 42));
        builder.setChildNode("c").setProperty(binaryPropertyFromBlob("bins", createBlobs(nodeStore, 42, 43, 44)));
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static void modifyTestContent(NodeStore nodeStore) throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.getChildNode("a").getChildNode("aa").remove();
        builder.getChildNode("b").setProperty("bin", "changed");
        builder.getChildNode("c").removeProperty("bins");
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static Blob createBlob(NodeStore nodeStore, int size) throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

    private static List<Blob> createBlobs(NodeStore nodeStore, int... sizes) throws IOException {
        List<Blob> blobs = newArrayList();
        for (int size : sizes) {
            blobs.add(createBlob(nodeStore, size));
        }
        return blobs;
    }

}