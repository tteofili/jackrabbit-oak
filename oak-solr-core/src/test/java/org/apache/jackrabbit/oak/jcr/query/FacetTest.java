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
package org.apache.jackrabbit.oak.jcr.query;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;

import org.apache.jackrabbit.core.query.AbstractQueryTest;

/**
 * Test for faceting capabilities from the JCR spec point of view
 */
public class FacetTest extends AbstractQueryTest {

    public void testFacetRetrieval() throws Exception {
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("text", "hello");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("text", "hallo");
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty("text", "oh hallo");
        session.save();

        String sql2 = "select [jcr:path], [facet(text)] from [nt:base] " +
                "where contains([text], 'hello OR hallo') order by [jcr:path]";
        Query q = qm.createQuery(sql2, Query.JCR_SQL2);
        QueryResult result = q.execute();
        String facetResult = "text:[hallo (2), hello (1), oh (1)]";
        assertEquals(facetResult + ", " + facetResult + ", " + facetResult, getResult(result, "facet(text)"));
    }

    public void testFacetRetrieval4() throws Exception {
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("jcr:title", "apache jackrabbit oak");
        n1.setProperty("tags", new String[]{"software", "repository", "apache"});
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("jcr:title", "oak furniture");
        n2.setProperty("tags", "furniture");
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty("jcr:title", "oak cosmetics");
        n3.setProperty("tags", "cosmetics");
        Node n4 = testRootNode.addNode("node4");
        n4.setProperty("jcr:title", "oak and aem");
        n4.setProperty("tags", new String[]{"software", "repository", "aem"});
        session.save();

        String sql2 = "select [jcr:path], [facet(tags)] from [nt:base] " +
                "where contains([jcr:title], 'oak') order by [jcr:path]";
        Query q = qm.createQuery(sql2, Query.JCR_SQL2);
        QueryResult result = q.execute();
        String facetResult = "tags:[repository (2), software (2), aem (1), apache (1), cosmetics (1), furniture (1)], tags:[repository (2), software (2), aem (1), apache (1), cosmetics (1), furniture (1)], tags:[repository (2), software (2), aem (1), apache (1), cosmetics (1), furniture (1)], tags:[repository (2), software (2), aem (1), apache (1), cosmetics (1), furniture (1)]";
        assertEquals(facetResult, getResult(result, "facet(tags)"));
    }

    public void testFacetRetrievalWithAnonymousUser() throws Exception {
        Session session = superuser;

        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("text", "hello");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("text", "hallo");
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty("text", "oh hallo");
        session.save();

        session = getHelper().getReadOnlySession();
        QueryManager qm = session.getWorkspace().getQueryManager();

        String sql2 = "select [jcr:path], [facet(text)] from [nt:base] " +
                "where contains([text], 'hello OR hallo') order by [jcr:path]";
        Query q = qm.createQuery(sql2, Query.JCR_SQL2);
        QueryResult result = q.execute();
        String facetResult = "text:[hallo (2), hello (1), oh (1)]";
        assertEquals(facetResult + ", " + facetResult + ", " + facetResult, getResult(result, "facet(text)"));
    }

    public void testFacetRetrieval2() throws Exception {
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node n1 = testRootNode.addNode("node1");
        String pn = "jcr:title";
        n1.setProperty(pn, "hello");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty(pn, "hallo");
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty(pn, "oh hallo");
        session.save();

        String sql2 = "select [jcr:path], [facet(" + pn + ")] from [nt:base] " +
                "where contains([" + pn + "], 'hallo') order by [jcr:path]";
        Query q = qm.createQuery(sql2, Query.JCR_SQL2);
        QueryResult result = q.execute();
        String facetResult = pn + ":[hallo (2), oh (1)]";
        assertEquals(facetResult + ", " + facetResult, getResult(result, "facet(" + pn + ")"));
    }

    public void testFacetRetrieval3() throws Exception {
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node n1 = testRootNode.addNode("node1");
        String pn = "jcr:title";
        String pn2 = "jcr:description";
        n1.setProperty(pn, "hello");
        n1.setProperty(pn2, "a");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty(pn, "hallo");
        n2.setProperty(pn2, "b");
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty(pn, "oh hallo");
        n3.setProperty(pn2, "a");
        session.save();

        String sql2 = "select [jcr:path], [facet(" + pn + ")], [facet(" + pn2 + ")] from [nt:base] " +
                "where contains([" + pn + "], 'hallo') order by [jcr:path]";
        Query q = qm.createQuery(sql2, Query.JCR_SQL2);
        QueryResult result = q.execute();
        String facetResult = pn + ":[hallo (2), oh (1)], " + pn2 + ":[a (1), b (1)], " + pn + ":[hallo (2), oh (1)], " + pn2 + ":[a (1), b (1)]";
        assertEquals(facetResult, getResult(result, "facet(" + pn + ")", "facet(" + pn2 + ")"));
    }

    static String getResult(QueryResult result, String... propertyNames) throws RepositoryException {
        StringBuilder buff = new StringBuilder();
        RowIterator it = result.getRows();
        while (it.hasNext()) {

            Row row = it.nextRow();
            for (String propertyName : propertyNames) {
                Value value = row.getValue(propertyName);
                if (value != null) {
                    if (buff.length() > 0) {
                        buff.append(", ");
                    }
                    buff.append(value.getString());
                }
            }
        }
        return buff.toString();
    }

}