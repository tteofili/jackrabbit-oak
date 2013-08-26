/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLuceneIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.analysis.Analyzer;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class LuceneIndexTest {

    private static final Analyzer analyzer = LuceneIndexConstants.ANALYZER;

    private static final EditorHook HOOK = new EditorHook(
            new IndexUpdateProvider(new LuceneIndexEditorProvider().with(analyzer)));

    private NodeState root = new InitialContent().initialize(EMPTY_NODE);

    private NodeBuilder builder = root.builder();

    @Test
    public void testLucene() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLuceneIndexDefinition(index, "lucene", null);

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after);

        QueryIndex queryIndex = new LuceneIndex(analyzer, null);
        FilterImpl filter = createFilter(NT_BASE);
        filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        Cursor cursor = queryIndex.query(filter, indexed);
        assertTrue(cursor.hasNext());
        assertEquals("/", cursor.next().getPath());
        assertFalse(cursor.hasNext());
    }

    @Test
    public void testLucene2() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLuceneIndexDefinition(index, "lucene", null);

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        builder.child("a").setProperty("foo", "bar");
        builder.child("a").child("b").setProperty("foo", "bar");
        builder.child("a").child("b").child("c").setProperty("foo", "bar");

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after);

        QueryIndex queryIndex = new LuceneIndex(analyzer, null);
        FilterImpl filter = createFilter(NT_BASE);
        // filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        Cursor cursor = queryIndex.query(filter, indexed);

        assertTrue(cursor.hasNext());
        assertEquals("/a/b/c", cursor.next().getPath());
        assertEquals("/a/b", cursor.next().getPath());
        assertEquals("/a", cursor.next().getPath());
        assertEquals("/", cursor.next().getPath());
        assertFalse(cursor.hasNext());
    }

    @Test
    public void testLucene3() throws Exception {
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLuceneIndexDefinition(
                index, "lucene", ImmutableSet.of(PropertyType.TYPENAME_STRING));

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        builder.child("a").setProperty("foo", "bar");
        builder.child("a").child("b").setProperty("foo", "bar", Type.NAME);
        builder.child("a").child("b").child("c").setProperty("foo", "bar", Type.NAME);

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after);

        QueryIndex queryIndex = new LuceneIndex(analyzer, null);
        FilterImpl filter = createFilter(NT_BASE);
        // filter.restrictPath("/", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL,
                PropertyValues.newString("bar"));
        Cursor cursor = queryIndex.query(filter, indexed);

        assertTrue(cursor.hasNext());
        assertEquals("/a", cursor.next().getPath());
        assertEquals("/", cursor.next().getPath());
        assertFalse(cursor.hasNext());
    }

    private FilterImpl createFilter(String nodeTypeName) {
        NodeState system = root.getChildNode(JCR_SYSTEM);
        NodeState types = system.getChildNode(JCR_NODE_TYPES);
        NodeState type = types.getChildNode(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", null);
    }

}
