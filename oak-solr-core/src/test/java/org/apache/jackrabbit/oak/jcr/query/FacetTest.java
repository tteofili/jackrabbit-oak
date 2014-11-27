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
        assertEquals("text:[hallo (2), hello (1), oh (1)]", getResult(result, "facet(text)"));
    }

    static String getResult(QueryResult result, String propertyName) throws RepositoryException {
        StringBuilder buff = new StringBuilder();
        RowIterator it = result.getRows();
        while (it.hasNext()) {
            if (buff.length() > 0) {
                buff.append(", ");
            }
            Row row = it.nextRow();
            Value value = row.getValue(propertyName);
            System.out.println(value);
            if (value != null) {
                buff.append(value.getString());
            }
        }
        return buff.toString();
    }

}
