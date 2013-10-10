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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.JcrPathParser;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.query.ast.AndImpl;
import org.apache.jackrabbit.oak.query.ast.AstVisitorBase;
import org.apache.jackrabbit.oak.query.ast.BindVariableValueImpl;
import org.apache.jackrabbit.oak.query.ast.ChildNodeImpl;
import org.apache.jackrabbit.oak.query.ast.ChildNodeJoinConditionImpl;
import org.apache.jackrabbit.oak.query.ast.ColumnImpl;
import org.apache.jackrabbit.oak.query.ast.ComparisonImpl;
import org.apache.jackrabbit.oak.query.ast.ConstraintImpl;
import org.apache.jackrabbit.oak.query.ast.DescendantNodeImpl;
import org.apache.jackrabbit.oak.query.ast.DescendantNodeJoinConditionImpl;
import org.apache.jackrabbit.oak.query.ast.EquiJoinConditionImpl;
import org.apache.jackrabbit.oak.query.ast.FullTextSearchImpl;
import org.apache.jackrabbit.oak.query.ast.FullTextSearchScoreImpl;
import org.apache.jackrabbit.oak.query.ast.InImpl;
import org.apache.jackrabbit.oak.query.ast.LengthImpl;
import org.apache.jackrabbit.oak.query.ast.LiteralImpl;
import org.apache.jackrabbit.oak.query.ast.LowerCaseImpl;
import org.apache.jackrabbit.oak.query.ast.NodeLocalNameImpl;
import org.apache.jackrabbit.oak.query.ast.NodeNameImpl;
import org.apache.jackrabbit.oak.query.ast.NotImpl;
import org.apache.jackrabbit.oak.query.ast.OrImpl;
import org.apache.jackrabbit.oak.query.ast.OrderingImpl;
import org.apache.jackrabbit.oak.query.ast.PropertyExistenceImpl;
import org.apache.jackrabbit.oak.query.ast.PropertyInexistenceImpl;
import org.apache.jackrabbit.oak.query.ast.PropertyValueImpl;
import org.apache.jackrabbit.oak.query.ast.SameNodeImpl;
import org.apache.jackrabbit.oak.query.ast.SameNodeJoinConditionImpl;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.ast.SourceImpl;
import org.apache.jackrabbit.oak.query.ast.UpperCaseImpl;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a parsed query.
 */
public class QueryImpl implements Query {
    
    /**
     * The "jcr:path" pseudo-property.
     */
    // TODO jcr:path isn't an official feature, support it?
    public static final String JCR_PATH = "jcr:path";

    /**
     * The "jcr:score" pseudo-property.
     */
    public static final String JCR_SCORE = "jcr:score";

    /**
     * The "rep:excerpt" pseudo-property.
     */
    public static final String REP_EXCERPT = "rep:excerpt";

    private static final Logger LOG = LoggerFactory.getLogger(QueryImpl.class);

    final SourceImpl source;
    final String statement;
    final HashMap<String, PropertyValue> bindVariableMap = new HashMap<String, PropertyValue>();
    final HashMap<String, Integer> selectorIndexes = new HashMap<String, Integer>();
    final ArrayList<SelectorImpl> selectors = new ArrayList<SelectorImpl>();
    ConstraintImpl constraint;
    
    private QueryEngineImpl queryEngine;
    private OrderingImpl[] orderings;
    private ColumnImpl[] columns;
    private boolean explain, measure;
    private boolean distinct;
    private long limit = Long.MAX_VALUE;
    private long offset;
    private long size = -1;
    private boolean prepared;
    private Tree rootTree;
    private NodeState rootState;
    private NamePathMapper namePathMapper;

    QueryImpl(String statement, SourceImpl source, ConstraintImpl constraint, ColumnImpl[] columns) {
        this.statement = statement;
        this.source = source;
        this.constraint = constraint;
        this.columns = columns;
    }

    @Override
    public void init() {

        final QueryImpl query = this;

        new AstVisitorBase() {

            @Override
            public boolean visit(BindVariableValueImpl node) {
                node.setQuery(query);
                bindVariableMap.put(node.getBindVariableName(), null);
                return true;
            }
            
             @Override
            public boolean visit(ChildNodeImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(ChildNodeJoinConditionImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(ColumnImpl node) {
                node.setQuery(query);
                return true;
            }

            @Override
            public boolean visit(DescendantNodeImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(DescendantNodeJoinConditionImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(EquiJoinConditionImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(FullTextSearchImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return super.visit(node);
            }

            @Override
            public boolean visit(FullTextSearchScoreImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(LiteralImpl node) {
                node.setQuery(query);
                return true;
            }

            @Override
            public boolean visit(NodeLocalNameImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(NodeNameImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(PropertyExistenceImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }
            
            @Override
            public boolean visit(PropertyInexistenceImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(PropertyValueImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(SameNodeImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(SameNodeJoinConditionImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(SelectorImpl node) {
                String name = node.getSelectorName();
                if (selectorIndexes.put(name, selectors.size()) != null) {
                    throw new IllegalArgumentException("Two selectors with the same name: " + name);
                }
                selectors.add(node);
                node.setQuery(query);
                return true;
            }

            @Override
            public boolean visit(LengthImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

            @Override
            public boolean visit(UpperCaseImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

            @Override
            public boolean visit(LowerCaseImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

            @Override
            public boolean visit(ComparisonImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

            @Override
            public boolean visit(InImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }
            
            @Override
            public boolean visit(AndImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

            @Override
            public boolean visit(OrImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

            @Override
            public boolean visit(NotImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

        }.visit(this);
        if (constraint != null) {
            constraint = constraint.simplify();
        }
        source.setQueryConstraint(constraint);
        source.init(this);
        for (ColumnImpl column : columns) {
            column.bindSelector(source);
        }
    }

    @Override
    public ColumnImpl[] getColumns() {
        return columns;
    }

    public ConstraintImpl getConstraint() {
        return constraint;
    }

    public OrderingImpl[] getOrderings() {
        return orderings;
    }

    public SourceImpl getSource() {
        return source;
    }

    @Override
    public void bindValue(String varName, PropertyValue value) {
        bindVariableMap.put(varName, value);
    }

    @Override
    public void setLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public void setExplain(boolean explain) {
        this.explain = explain;
    }

    @Override
    public void setMeasure(boolean measure) {
        this.measure = measure;
    }
    
    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    @Override
    public ResultImpl executeQuery() {
        return new ResultImpl(this);
    }

    @Override
    public Iterator<ResultRowImpl> getRows() {
        prepare();
        if (explain) {
            String plan = getPlan();
            columns = new ColumnImpl[] { new ColumnImpl("explain", "plan", "plan")};
            ResultRowImpl r = new ResultRowImpl(this,
                    new String[0], 
                    new PropertyValue[] { PropertyValues.newString(plan)},
                    null);
            return Arrays.asList(r).iterator();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("query execute {} ", statement);
            LOG.debug("query plan {}", getPlan());
        }
        RowIterator rowIt = new RowIterator(rootState);
        Comparator<ResultRowImpl> orderBy = ResultRowImpl.getComparator(orderings);
        Iterator<ResultRowImpl> it = 
                FilterIterators.newCombinedFilter(rowIt, distinct, limit, offset, orderBy);
        if (measure) {
            // run the query
            while (it.hasNext()) {
                it.next();
            }
            columns = new ColumnImpl[] {
                    new ColumnImpl("measure", "selector", "selector"),
                    new ColumnImpl("measure", "scanCount", "scanCount")
            };
            ArrayList<ResultRowImpl> list = new ArrayList<ResultRowImpl>();
            ResultRowImpl r = new ResultRowImpl(this,
                    new String[0],
                    new PropertyValue[] {
                            PropertyValues.newString("query"),
                            PropertyValues.newLong(rowIt.getReadCount())
                        },
                    null);
            list.add(r);
            for (SelectorImpl selector : selectors) {
                r = new ResultRowImpl(this,
                        new String[0],
                        new PropertyValue[] {
                                PropertyValues.newString(selector.getSelectorName()),
                                PropertyValues.newLong(selector.getScanCount()),
                            },
                        null);
                list.add(r);
            }
            it = list.iterator();
        }
        return it;
    }
    
    @Override
    public String getPlan() {
        return source.getPlan(rootState);
    }

    @Override
    public void prepare() {
        if (prepared) {
            return;
        }
        prepared = true;
        source.prepare();
    }

    /**
     * An iterator over result rows.
     */
    class RowIterator implements Iterator<ResultRowImpl> {

        private final NodeState rootState;
        private ResultRowImpl current;
        private boolean started, end;
        private long rowIndex;

        RowIterator(NodeState rootState) {
            this.rootState = rootState;
        }

        public long getReadCount() {
            return rowIndex;
        }

        private void fetchNext() {
            if (end) {
                return;
            }
            if (!started) {
                source.execute(rootState);
                started = true;
            }
            while (true) {
                if (source.next()) {
                    if (constraint == null || constraint.evaluate()) {
                        current = currentRow();
                        rowIndex++;
                        break;
                    }
                } else {
                    current = null;
                    end = true;
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (end) {
                return false;
            }
            if (current == null) {
                fetchNext();
            }
            return !end;
        }

        @Override
        public ResultRowImpl next() {
            if (end) {
                return null;
            }
            if (current == null) {
                fetchNext();
            }
            ResultRowImpl r = current;
            current = null;
            return r;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    ResultRowImpl currentRow() {
        int selectorCount = selectors.size();
        String[] paths = new String[selectorCount];
        for (int i = 0; i < selectorCount; i++) {
            SelectorImpl s = selectors.get(i);
            paths[i] = s.currentPath();
        }
        int columnCount = columns.length;
        PropertyValue[] values = new PropertyValue[columnCount];
        for (int i = 0; i < columnCount; i++) {
            ColumnImpl c = columns[i];
            values[i] = c.currentProperty();
        }
        PropertyValue[] orderValues;
        if (orderings == null) {
            orderValues = null;
        } else {
            int size = orderings.length;
            orderValues = new PropertyValue[size];
            for (int i = 0; i < size; i++) {
                orderValues[i] = orderings[i].getOperand().currentProperty();
            }
        }
        return new ResultRowImpl(this, paths, values, orderValues);
    }

    @Override
    public int getSelectorIndex(String selectorName) {
        Integer index = selectorIndexes.get(selectorName);
        if (index == null) {
            throw new IllegalArgumentException("Unknown selector: " + selectorName);
        }
        return index;
    }

    @Override
    public int getColumnIndex(String columnName) {
        return getColumnIndex(columns, columnName);
    }
    
    static int getColumnIndex(ColumnImpl[] columns, String columnName) {
        for (int i = 0, size = columns.length; i < size; i++) {
            ColumnImpl c = columns[i];
            String cn = c.getColumnName();
            if (cn != null && cn.equals(columnName)) {
                return i;
            }
        }
        return -1;
    }

    public PropertyValue getBindVariableValue(String bindVariableName) {
        PropertyValue v = bindVariableMap.get(bindVariableName);
        if (v == null) {
            throw new IllegalArgumentException("Bind variable value not set: " + bindVariableName);
        }
        return v;
    }

    @Override
    public String[] getSelectorNames() {
        String[] list = new String[selectors.size()];
        for (int i = 0; i < list.length; i++) {
            list[i] = selectors.get(i).getSelectorName();
        }
        return list;
    }

    @Override
    public List<String> getBindVariableNames() {
        return new ArrayList<String>(bindVariableMap.keySet());
    }

    @Override
    public void setQueryEngine(QueryEngineImpl queryEngine) {
        this.queryEngine = queryEngine;
    }

    public QueryIndex getBestIndex(Filter filter) {
        return queryEngine.getBestIndex(this, rootState, filter);
    }

    @Override
    public void setRootTree(Tree rootTree) {
        this.rootTree = rootTree;
    }
    
    public Tree getRootTree() {
        return rootTree;
    }
    
    @Override
    public void setRootState(NodeState rootState) {
        this.rootState = rootState;
    }
    
    @Override
    public void setOrderings(OrderingImpl[] orderings) {
        this.orderings = orderings;
    }

    @Override
    public void setNamePathMapper(NamePathMapper namePathMapper) {
        this.namePathMapper = namePathMapper;
    }

    public NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    @Override
    public Tree getTree(String path) {
        return TreeUtil.getTree(rootTree, PathUtils.isAbsolute(path) ? path.substring(1) : path);
    }

    /**
     * Validate a path is syntactically correct.
     * 
     * @param path the path to validate
     */
    public void validatePath(String path) {
        if (!JcrPathParser.validate(path)) {
            throw new IllegalArgumentException("Invalid path: " + path);
        }
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("select ");
        int i = 0;
        for (ColumnImpl c : columns) {
            if (i++ > 0) {
                buff.append(", ");
            }
            buff.append(c);
        }
        buff.append(" from ").append(source);
        if (constraint != null) {
            buff.append(" where ").append(constraint);
        }
        if (orderings != null) {
            buff.append(" order by ");
            i = 0;
            for (OrderingImpl o : orderings) {
                if (i++ > 0) {
                    buff.append(", ");
                }
                buff.append(o);
            }
        }
        return buff.toString();
    }

    @Override
    public long getSize() {
        return size;
    }

    public String getStatement() {
        return statement;
    }

}
