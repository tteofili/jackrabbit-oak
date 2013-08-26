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
package org.apache.jackrabbit.oak.kernel;

import static junit.framework.Assert.assertFalse;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.jackrabbit.oak.NodeStoreFixture;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.commit.PostCommitHook;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class NodeStoreTest {

    @Parameters
    public static Collection<Object[]> fixtures() {
        Object[][] fixtures = new Object[][] {
                {NodeStoreFixture.MK_IMPL},
                {NodeStoreFixture.MONGO_MK},
                {NodeStoreFixture.SEGMENT_MK},
        };
        return Arrays.asList(fixtures);
    }

    private NodeStore store;

    private NodeState root;

    private NodeStoreFixture fixture;

    public NodeStoreTest(NodeStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Before
    public void setUp() throws Exception {
        store = fixture.createNodeStore();
        NodeStoreBranch branch = store.branch();
        NodeBuilder builder = branch.getHead().builder();
        NodeBuilder test = builder.child("test");
        test.setProperty("a", 1);
        test.setProperty("b", 2);
        test.setProperty("c", 3);
        test.child("x");
        test.child("y");
        test.child("z");
        branch.setRoot(builder.getNodeState());
        branch.merge(EmptyHook.INSTANCE, PostCommitHook.EMPTY);
        root = store.getRoot();
    }

    @After
    public void tearDown() throws Exception {
        fixture.dispose(store);
    }

    @Test
    public void getRoot() {
        assertEquals(root, store.getRoot());
        assertEquals(root.getChildNode("test"), store.getRoot().getChildNode("test"));
        assertEquals(root.getChildNode("test").getChildNode("x"),
                store.getRoot().getChildNode("test").getChildNode("x"));
        assertEquals(root.getChildNode("test").getChildNode("any"),
                store.getRoot().getChildNode("test").getChildNode("any"));
        assertEquals(root.getChildNode("test").getProperty("a"),
                store.getRoot().getChildNode("test").getProperty("a"));
        assertEquals(root.getChildNode("test").getProperty("any"),
                store.getRoot().getChildNode("test").getProperty("any"));
    }

    @Test
    public void branch() throws CommitFailedException {
        NodeStoreBranch branch = store.branch();

        NodeBuilder rootBuilder = branch.getHead().builder();
        NodeBuilder testBuilder = rootBuilder.child("test");
        NodeBuilder newNodeBuilder = testBuilder.child("newNode");

        testBuilder.getChildNode("x").remove();

        newNodeBuilder.setProperty("n", 42);

        // Assert changes are present in the builder
        NodeState testState = rootBuilder.getNodeState().getChildNode("test");
        assertTrue(testState.getChildNode("newNode").exists());
        assertFalse(testState.getChildNode("x").exists());
        assertEquals(42, (long) testState.getChildNode("newNode").getProperty("n").getValue(LONG));

        // Assert changes are not yet present in the branch
        testState = branch.getHead().getChildNode("test");
        assertFalse(testState.getChildNode("newNode").exists());
        assertTrue(testState.getChildNode("x").exists());

        branch.setRoot(rootBuilder.getNodeState());

        // Assert changes are present in the branch
        testState = branch.getHead().getChildNode("test");
        assertTrue(testState.getChildNode("newNode").exists());
        assertFalse(testState.getChildNode("x").exists());
        assertEquals(42, (long) testState.getChildNode("newNode").getProperty("n").getValue(LONG));

        // Assert changes are not yet present in the trunk
        testState = store.getRoot().getChildNode("test");
        assertFalse(testState.getChildNode("newNode").exists());
        assertTrue(testState.getChildNode("x").exists());

        branch.merge(EmptyHook.INSTANCE, PostCommitHook.EMPTY);

        // Assert changes are present in the trunk
        testState = store.getRoot().getChildNode("test");
        assertTrue(testState.getChildNode("newNode").exists());
        assertFalse(testState.getChildNode("x").exists());
        assertEquals(42, (long) testState.getChildNode("newNode").getProperty("n").getValue(LONG));
    }

    @Test
    public void afterCommitHook() throws CommitFailedException {
        // this test only works with a KernelNodeStore
        assumeTrue(store instanceof KernelNodeStore);
        final NodeState[] states = new NodeState[2]; // { before, after }
        ((KernelNodeStore) store).setObserver(new Observer() {
            @Override
            public void contentChanged(NodeState before, NodeState after) {
                states[0] = before;
                states[1] = after;
            }
        });

        NodeState root = store.getRoot();
        NodeBuilder rootBuilder= root.builder();
        NodeBuilder testBuilder = rootBuilder.child("test");
        NodeBuilder newNodeBuilder = testBuilder.child("newNode");

        newNodeBuilder.setProperty("n", 42);

        testBuilder.getChildNode("a").remove();

        NodeState newRoot = rootBuilder.getNodeState();

        NodeStoreBranch branch = store.branch();
        branch.setRoot(newRoot);
        branch.merge(EmptyHook.INSTANCE, PostCommitHook.EMPTY);
        store.getRoot(); // triggers the observer

        NodeState before = states[0];
        NodeState after = states[1];
        assertNotNull(before);
        assertNotNull(after);

        assertFalse(before.getChildNode("test").getChildNode("newNode").exists());
        assertTrue(after.getChildNode("test").getChildNode("newNode").exists());
        assertFalse(after.getChildNode("test").getChildNode("a").exists());
        assertEquals(42, (long) after.getChildNode("test").getChildNode("newNode").getProperty("n").getValue(LONG));
        assertEquals(newRoot, after);
    }

    @Test
    public void beforeCommitHook() throws CommitFailedException {
        NodeState root = store.getRoot();
        NodeBuilder rootBuilder = root.builder();
        NodeBuilder testBuilder = rootBuilder.child("test");
        NodeBuilder newNodeBuilder = testBuilder.child("newNode");

        newNodeBuilder.setProperty("n", 42);

        testBuilder.getChildNode("a").remove();

        NodeState newRoot = rootBuilder.getNodeState();

        NodeStoreBranch branch = store.branch();
        branch.setRoot(newRoot);
        branch.merge(new CommitHook() {
            @Override
            public NodeState processCommit(NodeState before, NodeState after) {
                NodeBuilder rootBuilder = after.builder();
                NodeBuilder testBuilder = rootBuilder.child("test");
                testBuilder.child("fromHook");
                return rootBuilder.getNodeState();
            }
        }, PostCommitHook.EMPTY);

        NodeState test = store.getRoot().getChildNode("test");
        assertTrue(test.getChildNode("newNode").exists());
        assertTrue(test.getChildNode("fromHook").exists());
        assertFalse(test.getChildNode("a").exists());
        assertEquals(42, (long) test.getChildNode("newNode").getProperty("n").getValue(LONG));
        assertEquals(test, store.getRoot().getChildNode("test"));
    }

    @Test
    public void manyChildNodes() throws CommitFailedException {
        NodeStoreBranch branch = store.branch();
        NodeBuilder root = branch.getHead().builder();
        NodeBuilder parent = root.child("parent");
        for (int i = 0; i <= KernelNodeState.MAX_CHILD_NODE_NAMES; i++) {
            parent.child("child-" + i);
        }
        branch.setRoot(root.getNodeState());
        branch.merge(EmptyHook.INSTANCE, PostCommitHook.EMPTY);

        NodeState base = store.getRoot();
        branch = store.branch();
        root = branch.getHead().builder();
        parent = root.child("parent");
        parent.child("child-new");
        branch.setRoot(root.getNodeState());
        branch.merge(EmptyHook.INSTANCE, PostCommitHook.EMPTY);

        Diff diff = new Diff();
        store.getRoot().compareAgainstBaseState(base, diff);

        assertEquals(0, diff.removed.size());
        assertEquals(1, diff.added.size());
        assertEquals("child-new", diff.added.get(0));

        base = store.getRoot();
        branch = store.branch();
        branch.move("/parent/child-new", "/parent/child-moved");
        branch.merge(EmptyHook.INSTANCE, PostCommitHook.EMPTY);

        diff = new Diff();
        store.getRoot().compareAgainstBaseState(base, diff);

        assertEquals(1, diff.removed.size());
        assertEquals("child-new", diff.removed.get(0));
        assertEquals(1, diff.added.size());
        assertEquals("child-moved", diff.added.get(0));

        base = store.getRoot();
        branch = store.branch();
        root = branch.getHead().builder();
        parent = root.child("parent");
        parent.child("child-moved").setProperty("foo", "value");
        parent.child("child-moved").setProperty(
                new MultiStringPropertyState("bar", Arrays.asList("value")));
        branch.setRoot(root.getNodeState());
        branch.merge(EmptyHook.INSTANCE, PostCommitHook.EMPTY);

        diff = new Diff();
        store.getRoot().compareAgainstBaseState(base, diff);

        assertEquals(0, diff.removed.size());
        assertEquals(0, diff.added.size());
        assertEquals(2, diff.addedProperties.size());
        assertTrue(diff.addedProperties.contains("foo"));
        assertTrue(diff.addedProperties.contains("bar"));

        base = store.getRoot();
        branch = store.branch();
        root = branch.getHead().builder();
        parent = root.child("parent");
        parent.setProperty("foo", "value");
        parent.setProperty(new MultiStringPropertyState(
                "bar", Arrays.asList("value")));
        branch.setRoot(root.getNodeState());
        branch.merge(EmptyHook.INSTANCE, PostCommitHook.EMPTY);

        diff = new Diff();
        store.getRoot().compareAgainstBaseState(base, diff);

        assertEquals(0, diff.removed.size());
        assertEquals(0, diff.added.size());
        assertEquals(2, diff.addedProperties.size());
        assertTrue(diff.addedProperties.contains("foo"));
        assertTrue(diff.addedProperties.contains("bar"));

        base = store.getRoot();
        branch = store.branch();
        root = branch.getHead().builder();
        parent = root.child("parent");
        parent.getChildNode("child-moved").remove();
        branch.setRoot(root.getNodeState());
        branch.merge(EmptyHook.INSTANCE, PostCommitHook.EMPTY);

        diff = new Diff();
        store.getRoot().compareAgainstBaseState(base, diff);

        assertEquals(1, diff.removed.size());
        assertEquals(0, diff.added.size());
        assertEquals("child-moved", diff.removed.get(0));
    }

    @Test
    public void moveToSelf() throws CommitFailedException {
        NodeStoreBranch branch = store.branch();
        assertTrue(branch.move("/x", "/x"));
    }

    @Test
    public void oak965() throws CommitFailedException {
        NodeStore store1 = init(fixture.createNodeStore());
        NodeStore store2 = init(fixture.createNodeStore());
        try {
            NodeState tree1 = store1.getRoot();
            NodeState tree2 = store2.getRoot();
            tree1.equals(tree2);
        } finally {
            fixture.dispose(store1);
            fixture.dispose(store2);
        }
    }

    private static NodeStore init(NodeStore store) throws CommitFailedException {
        NodeStoreBranch branch = store.branch();
        NodeBuilder builder = branch.getHead().builder();
        builder.setChildNode("root");
        branch.setRoot(builder.getNodeState());
        branch.merge(EmptyHook.INSTANCE, PostCommitHook.EMPTY);
        return store;
    }

    @Test
    public void compareAgainstBaseState0() throws CommitFailedException {
        compareAgainstBaseState(0);
    }

    @Test
    public void compareAgainstBaseState20() throws CommitFailedException {
        compareAgainstBaseState(20);
    }

    @Test
    public void compareAgainstBaseState100() throws CommitFailedException {
        compareAgainstBaseState(KernelNodeState.MAX_CHILD_NODE_NAMES);
    }

    private void compareAgainstBaseState(int childNodeCount) throws CommitFailedException {
        NodeStoreBranch branch = store.branch();

        NodeState before = branch.getHead();
        NodeBuilder builder = before.builder();
        for (int k = 0; k < childNodeCount; k++) {
            builder.child("c" + k);
        }

        builder.child("foo").child(":bar").child("quz").setProperty("p", "v");
        branch.setRoot(builder.getNodeState());
        branch.merge(EmptyHook.INSTANCE, PostCommitHook.EMPTY);

        NodeState after = store.getRoot();
        Diff diff = new Diff();
        after.compareAgainstBaseState(before, diff);

        assertEquals(0, diff.removed.size());
        assertEquals(childNodeCount + 1, diff.added.size());
        assertEquals(0, diff.addedProperties.size());
    }

    private static class Diff extends DefaultNodeStateDiff {

        List<String> addedProperties = new ArrayList<String>();
        List<String> added = new ArrayList<String>();
        List<String> removed = new ArrayList<String>();

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            added.add(name);
            return true;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            removed.add(name);
            return true;
        }

        @Override
        public boolean childNodeChanged(
                String name, NodeState before, NodeState after) {
            return after.compareAgainstBaseState(before, this);
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            addedProperties.add(after.getName());
            return true;
        }
    }

}
