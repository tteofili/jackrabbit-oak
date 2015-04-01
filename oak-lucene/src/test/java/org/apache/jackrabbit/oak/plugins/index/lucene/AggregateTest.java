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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.util.Collection;
import java.util.Map;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.aggregate.Aggregate;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_RULES;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.matchers.JUnitMatchers.hasItems;

public class AggregateTest {

    private final TestCollector col = new TestCollector();
    private final NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = root.builder();

    //~---------------------------------< Prop Includes >

    @Test
    public void propOneLevelNamed() throws Exception {
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        child(rules, "nt:folder/properties/p1")
                .setProperty(LuceneIndexConstants.PROP_NAME, "a/p1");

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        Aggregate ag = defn.getApplicableIndexingRule("nt:folder").getAggregate();

        NodeBuilder nb = newNode("nt:folder");
        nb.child("a").setProperty("p1", "foo");
        nb.child("a").setProperty("p2", "foo");
        nb.child("b").setProperty("p2", "foo");

        ag.collectAggregates(nb.getNodeState(), col);
        assertEquals(1, col.getPropPaths().size());
        assertThat(col.getPropPaths(), hasItems("a/p1"));
    }

    @Test
    public void propOneLevelRegex() throws Exception {
        NodeBuilder rules = builder.child(INDEX_RULES);
        rules.child("nt:folder");
        child(rules, "nt:folder/properties/p1")
                .setProperty(LuceneIndexConstants.PROP_NAME, "a/foo.*")
                .setProperty(LuceneIndexConstants.PROP_IS_REGEX, true);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        Aggregate ag = defn.getApplicableIndexingRule("nt:folder").getAggregate();

        NodeBuilder nb = newNode("nt:folder");
        nb.child("a").setProperty("foo1", "foo");
        nb.child("a").setProperty("foo2", "foo");
        nb.child("a").setProperty("bar1", "foo");
        nb.child("b").setProperty("p2", "foo");

        ag.collectAggregates(nb.getNodeState(), col);
        assertEquals(2, col.getPropPaths().size());
        assertThat(col.getPropPaths(), hasItems("a/foo1", "a/foo2"));
    }

    //~---------------------------------< IndexingConfig >

    @Test
    public void simpleAggregateConfig() throws Exception {
        NodeBuilder aggregates = builder.child(LuceneIndexConstants.AGGREGATES);
        NodeBuilder aggFolder = aggregates.child("nt:folder");
        aggFolder.child("i1").setProperty(LuceneIndexConstants.AGG_PATH, "*");

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        Aggregate agg = defn.getAggregate("nt:folder");
        assertNotNull(agg);
        assertEquals(1, agg.getIncludes().size());
    }

    @Test
    public void aggregateConfig2() throws Exception {
        NodeBuilder aggregates = builder.child(LuceneIndexConstants.AGGREGATES);
        NodeBuilder aggFolder = aggregates.child("nt:folder");
        aggFolder.setProperty(LuceneIndexConstants.AGG_RECURSIVE_LIMIT, 42);
        aggFolder.child("i1").setProperty(LuceneIndexConstants.AGG_PATH, "*");
        aggFolder.child("i1").setProperty(LuceneIndexConstants.AGG_PRIMARY_TYPE, "nt:file");
        aggFolder.child("i1").setProperty(LuceneIndexConstants.AGG_RELATIVE_NODE, true);

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        Aggregate agg = defn.getAggregate("nt:folder");
        assertNotNull(agg);
        assertEquals(42, agg.reAggregationLimit);
        assertEquals(1, agg.getIncludes().size());
        assertEquals("nt:file", ((Aggregate.NodeInclude) agg.getIncludes().get(0)).primaryType);
        assertTrue(((Aggregate.NodeInclude) agg.getIncludes().get(0)).relativeNode);
    }

    private static NodeBuilder newNode(String typeName) {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(JCR_PRIMARYTYPE, typeName);
        return builder;
    }

    private static NodeBuilder child(NodeBuilder nb, String path) {
        for (String name : PathUtils.elements(checkNotNull(path))) {
            nb = nb.child(name);
        }
        return nb;
    }

    private static class TestCollector implements Aggregate.ResultCollector {
        final ListMultimap<String, Aggregate.NodeIncludeResult> nodeResults = ArrayListMultimap.create();
        final Map<String, Aggregate.PropertyIncludeResult> propResults = newHashMap();

        @Override
        public void onResult(Aggregate.NodeIncludeResult result) throws CommitFailedException {
            nodeResults.put(result.nodePath, result);
        }

        @Override
        public void onResult(Aggregate.PropertyIncludeResult result) throws CommitFailedException {
            propResults.put(result.propertyPath, result);

        }

        public Collection<String> getPropPaths() {
            return propResults.keySet();
        }

    }

}
