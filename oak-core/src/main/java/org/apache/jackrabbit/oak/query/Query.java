/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.query;

import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.query.ast.ColumnImpl;
import org.apache.jackrabbit.oak.query.ast.OrderingImpl;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A "select" or "union" query.
 * <p>
 * Lifecycle: use the constructor to create a new object. Call init() to
 * initialize the bind variable map. If the query is re-executed, a new instance
 * is created.
 */
public interface Query {

    void setRootTree(Tree rootTree);

    void setRootState(NodeState rootState);

    void setNamePathMapper(NamePathMapper namePathMapper);

    void setLimit(long limit);

    void setOffset(long offset);

    void bindValue(String key, PropertyValue value);

    void setQueryEngine(QueryEngineImpl queryEngineImpl);

    void prepare();

    Result executeQuery();

    List<String> getBindVariableNames();

    ColumnImpl[] getColumns();
    
    int getColumnIndex(String columnName);

    String[] getSelectorNames();

    int getSelectorIndex(String selectorName);

    Iterator<ResultRowImpl> getRows();

    long getSize();

    void setExplain(boolean explain);

    void setMeasure(boolean measure);

    void init();

    void setOrderings(OrderingImpl[] orderings);
    
    /**
     * Get the query plan. The query must already be prepared.
     * 
     * @return the query plan
     */
    String getPlan();

    Tree getTree(String path);

}
