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
package org.apache.jackrabbit.oak.plugins.index.solr.query;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.solr.SolrBaseTest;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.junit.Test;

/**
 * Integration test for {@link org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex} and {@link org.apache.jackrabbit.oak.plugins.index.solr.index.SolrCommitHook} working
 * together
 */
public class SolrQueryEngineIT extends SolrBaseTest {

    @Test
    public void testSolrQueryEngine() throws Exception {
        Root root = createRoot();
        Tree tree = root.getTree("/");
        tree.addChild("somenode").setProperty("foo", "bar");
        root.commit();

        QueryIndex index = new SolrQueryIndex("solr", server, configuration);
        FilterImpl filter = new FilterImpl(null, null);
        filter.restrictPath("/somenode", Filter.PathRestriction.EXACT);
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        Cursor cursor = index.query(filter, store.getRoot());
        assertNotNull(cursor);
        assertTrue(cursor.hasNext());
        assertEquals("/somenode", cursor.next().getPath());
        assertFalse(cursor.hasNext());
    }

}
