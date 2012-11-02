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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexHookManager;
import org.apache.jackrabbit.oak.plugins.nodetype.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests the query engine using the default index implementation: the
 * {@link LuceneIndexProvider}
 */
public class LuceneIndexQueryTest extends AbstractQueryTest {

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree index = root.getTree("/");
        createTestIndexNode(index, LuceneIndexConstants.TYPE_LUCENE);
        root.commit();
    }

    @Override
    protected ContentRepository createRepository() {
        return new Oak()
            .with(new InitialContent())
            .with(new LuceneIndexProvider())
            .with(new IndexHookManager(new LuceneIndexHookProvider()))
            .createContentRepository();
    }
    
    @Test
    @Ignore("OAK-420")
    public void sql2() throws Exception {
        test("sql2.txt");
    }

    @Test
    @Ignore("OAK-420")
    public void sql2Measure() throws Exception {
        test("sql2_measure.txt");
    }


}