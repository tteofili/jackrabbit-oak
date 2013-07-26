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
package org.apache.jackrabbit.oak.query;

import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.query.index.TraversingIndex;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The query engine implementation.
 */
public abstract class QueryEngineImpl implements QueryEngine {

    static final String SQL2 = "JCR-SQL2";
    static final String SQL = "sql";
    static final String XPATH = "xpath";
    static final String JQOM = "JCR-JQOM";

    static final String NO_LITERALS = "-noLiterals";

    static final Logger LOG = LoggerFactory.getLogger(QueryEngineImpl.class);
    
    private static final Set<String> SUPPORTED_LANGUAGES = of(
            SQL2,  SQL2  + NO_LITERALS,
            SQL,   SQL   + NO_LITERALS,
            XPATH, XPATH + NO_LITERALS,
            JQOM);

    private final QueryIndexProvider indexProvider;
    
    /**
     * Whether fallback to the traversing index is supported if no other index
     * is available. This is enabled by default and can be disabled for testing
     * purposes.
     */
    private boolean traversalFallback = true;

    public QueryEngineImpl(QueryIndexProvider indexProvider) {
        this.indexProvider = indexProvider;
    }

    /**
     * Get the current root node state, to run the query against.
     * 
     * @return the node state
     */
    protected abstract NodeState getRootState();

    /**
     * Get the current root tree, to run the query against.
     * 
     * @return the node state
     */
    protected abstract Tree getRootTree();

    @Override
    public Set<String> getSupportedQueryLanguages() {
        return SUPPORTED_LANGUAGES;
    }

    /**
     * Parse the query (check if it's valid) and get the list of bind variable names.
     *
     * @param statement
     * @param language
     * @return the list of bind variable names
     * @throws ParseException
     */
    @Override
    public List<String> getBindVariableNames(String statement, String language) throws ParseException {
        Query q = parseQuery(statement, language);
        return q.getBindVariableNames();
    }

    private Query parseQuery(String statement, String language) throws ParseException {
        LOG.debug("Parsing {} statement: {}", language, statement);
        NodeState root = getRootState();
        NodeState system = root.getChildNode(JCR_SYSTEM);
        NodeState types = system.getChildNode(JCR_NODE_TYPES);
        SQL2Parser parser = new SQL2Parser(types);
        if (language.endsWith(NO_LITERALS)) {
            language = language.substring(0, language.length() - NO_LITERALS.length());
            parser.setAllowNumberLiterals(false);
            parser.setAllowTextLiterals(false);
        }
        if (SQL2.equals(language) || JQOM.equals(language)) {
            return parser.parse(statement);
        } else if (SQL.equals(language)) {
            parser.setSupportSQL1(true);
            return parser.parse(statement);
        } else if (XPATH.equals(language)) {
            XPathToSQL2Converter converter = new XPathToSQL2Converter();
            String sql2 = converter.convert(statement);
            LOG.debug("XPath > SQL2: {}", sql2);
            try {
                return parser.parse(sql2);
            } catch (ParseException e) {
                throw new ParseException(statement + " converted to SQL-2 " + e.getMessage(), 0);
            }
        } else {
            throw new ParseException("Unsupported language: " + language, 0);
        }
    }
    
    @Override
    public Result executeQuery(String statement, String language, long limit,
            long offset, Map<String, ? extends PropertyValue> bindings,
            NamePathMapper namePathMapper) throws ParseException {
        if (limit < 0) {
            throw new IllegalArgumentException("Limit may not be negative, is: " + limit);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset may not be negative, is: " + offset);
        }
        Query q = parseQuery(statement, language);
        q.setRootTree(getRootTree());
        q.setRootState(getRootState());
        q.setNamePathMapper(namePathMapper);
        q.setLimit(limit);
        q.setOffset(offset);
        if (bindings != null) {
            for (Entry<String, ? extends PropertyValue> e : bindings.entrySet()) {
                q.bindValue(e.getKey(), e.getValue());
            }
        }
        q.setQueryEngine(this);
        q.prepare();
        return q.executeQuery();
    }

    protected void setTravesalFallback(boolean traversal) {
        this.traversalFallback = traversal;
    }

    public QueryIndex getBestIndex(QueryImpl query, NodeState rootState, Filter filter) {
        QueryIndex best = null;
        if (LOG.isDebugEnabled()) {
            LOG.debug("cost using filter " + filter);
        }
        List<QueryIndex> indexes = new ArrayList<QueryIndex>(
                indexProvider.getQueryIndexes(rootState));
        if (traversalFallback) {
            indexes.add(new TraversingIndex());
        }

        double bestCost = Double.POSITIVE_INFINITY;
        for (QueryIndex index : indexes) {
            double cost = index.getCost(filter, rootState);
            if (LOG.isDebugEnabled()) {
                LOG.debug("cost for " + index.getIndexName() + " is " + cost);
            }
            if (cost < bestCost) {
                bestCost = cost;
                best = index;
            }
        }
        return best;
    }

}
