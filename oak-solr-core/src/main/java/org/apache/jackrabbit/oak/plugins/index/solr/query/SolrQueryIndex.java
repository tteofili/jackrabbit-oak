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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.FulltextQueryIndex;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

import static org.apache.jackrabbit.oak.commons.PathUtils.getAncestorPath;
import static org.apache.jackrabbit.oak.commons.PathUtils.getDepth;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

/**
 * A Solr based {@link QueryIndex}
 */
public class SolrQueryIndex implements FulltextQueryIndex {

    public static final String TYPE = "solr";

    static final String NATIVE_SOLR_QUERY = "native*solr";

    static final String NATIVE_LUCENE_QUERY = "native*lucene";

    private final Logger log = LoggerFactory.getLogger(SolrQueryIndex.class);

    private final String name;
    private final SolrServer solrServer;
    private final OakSolrConfiguration configuration;

    private final NodeAggregator aggregator;

    public SolrQueryIndex(String name, SolrServer solrServer, OakSolrConfiguration configuration, NodeAggregator aggregator) {
        this.name = name;
        this.solrServer = solrServer;
        this.configuration = configuration;
        this.aggregator = aggregator;
    }

    public SolrQueryIndex(String name, SolrServer solrServer, OakSolrConfiguration configuration) {
        this(name, solrServer, configuration, null);
    }

    @Override
    public String getIndexName() {
        return name;
    }

    @Override
    public double getCost(Filter filter, NodeState root) {
        // cost is inverse proportional to the number of matching restrictions, infinite if no restriction matches
        double cost = 10d / getMatchingFilterRestrictions(filter);
        if (log.isDebugEnabled()) {
            log.debug("Solr: cost for {} is {}", name, cost);
        }
        return cost;
    }

    int getMatchingFilterRestrictions(Filter filter) {
        int match = 0;

        // full text expressions OR full text conditions defined
        if (filter.getFullTextConstraint() != null || (filter.getFulltextConditions() != null
                && filter.getFulltextConditions().size() > 0)) {
            match++; // full text queries have usually a significant recall
        }

        // path restriction defined AND path restrictions handled
        if (filter.getPathRestriction() != null &&
                !Filter.PathRestriction.NO_RESTRICTION.equals(filter.getPathRestriction())
                && configuration.useForPathRestrictions()) {
            match++;
        }

        // primary type restriction defined AND primary type restriction handled
        if (filter.getPrimaryTypes().size() > 0 && configuration.useForPrimaryTypes()) {
            match++;
        }

        // property restriction OR native language property restriction defined AND property restriction handled
        if (filter.getPropertyRestrictions() != null && filter.getPropertyRestrictions().size() > 0
                && (filter.getPropertyRestriction(NATIVE_SOLR_QUERY) != null || filter.getPropertyRestriction(NATIVE_LUCENE_QUERY) != null
                || configuration.useForPropertyRestrictions()) && !hasIgnoredProperties(filter.getPropertyRestrictions(), configuration)) {
            match++;
        }

        return match;
    }

    private static boolean hasIgnoredProperties(Collection<Filter.PropertyRestriction> propertyRestrictions, OakSolrConfiguration configuration) {
        for (Filter.PropertyRestriction pr : propertyRestrictions) {
            if (isIgnoredProperty(pr.propertyName, configuration)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getPlan(Filter filter, NodeState nodeState) {
        return FilterQueryParser.getQuery(filter, configuration, nodeState).toString();
    }

    /**
     * Get the set of relative paths of a full-text condition. For example, for
     * the condition "contains(a/b, 'hello') and contains(c/d, 'world'), the set
     * { "a", "c" } is returned. If there are no relative properties, then one
     * entry is returned (the empty string). If there is no expression, then an
     * empty set is returned.
     *
     * @param ft the full-text expression
     * @return the set of relative paths (possibly empty)
     */
    private static Set<String> getRelativePaths(FullTextExpression ft) {
        final HashSet<String> relPaths = new HashSet<String>();
        ft.accept(new FullTextVisitor.FullTextVisitorBase() {

            @Override
            public boolean visit(FullTextTerm term) {
                String p = term.getPropertyName();
                if (p == null) {
                    relPaths.add("");
                } else if (p.startsWith("../") || p.startsWith("./")) {
                    throw new IllegalArgumentException("Relative parent is not supported:" + p);
                } else if (getDepth(p) > 1) {
                    String parent = getParentPath(p);
                    relPaths.add(parent);
                } else {
                    relPaths.add("");
                }
                return true;
            }
        });
        return relPaths;
    }

    @Override
    public Cursor query(final Filter filter, final NodeState root) {
        Cursor cursor;
        try {
            final Set<String> relPaths = filter.getFullTextConstraint() != null ? getRelativePaths(filter.getFullTextConstraint())
                    : Collections.<String>emptySet();
            final String parent = relPaths.size() == 0 ? "" : relPaths.iterator().next();

            final int parentDepth = getDepth(parent);

            final List<FacetField> facetFields = new CopyOnWriteArrayList<FacetField>();

            cursor = new SolrRowCursor(new AbstractIterator<SolrResultRow>() {

                private final Set<String> seenPaths = Sets.newHashSet();
                private final Deque<SolrResultRow> queue = Queues.newArrayDeque();

                private SolrDocument lastDoc;

                public int offset = 0;

                @Override
                protected SolrResultRow computeNext() {
                    if (!queue.isEmpty() || loadDocs()) {
                        return queue.remove();
                    }
                    return endOfData();
                }

                private SolrResultRow convertToRow(SolrDocument doc) throws IOException {
                    String path = String.valueOf(doc.getFieldValue(configuration.getPathField()));
                    if (path != null) {
                        if ("".equals(path)) {
                            path = "/";
                        }
                        if (!parent.isEmpty()) {
                            path = getAncestorPath(path, parentDepth);
                            // avoid duplicate entries
                            if (seenPaths.contains(path)) {
                                return null;
                            }
                            seenPaths.add(path);
                        }

                        float score = 0f;
                        Object scoreObj = doc.get("score");
                        if (scoreObj != null) {
                            score = (Float) scoreObj;
                        }
                        return new SolrResultRow(path, score, doc, facetFields);
                    }
                    return null;
                }

                /**
                 * Loads the Solr documents in batches
                 * @return true if any document is loaded
                 */
                private boolean loadDocs() {
                    SolrDocument lastDocToRecord = null;

                    try {
                        if (log.isDebugEnabled()) {
                            log.debug("converting filter {}", filter);
                        }

                        SolrQuery query = FilterQueryParser.getQuery(filter, configuration, root);

                        if (lastDoc != null) {
                            offset++;
                            int newOffset = offset * configuration.getRows();
                            query.setParam("start", String.valueOf(newOffset));
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("sending query {}", query);
                        }

                        QueryResponse queryResponse = solrServer.query(query);

                        // get docs
                        SolrDocumentList docs = queryResponse.getResults();

                        // get facets
                        List<FacetField> returnedFieldFacet = queryResponse.getFacetFields();
                        if (returnedFieldFacet != null) {
                            facetFields.addAll(returnedFieldFacet);
                        }

                        // filter facets on doc paths
                        for (SolrDocument doc : docs) {
                            String path = String.valueOf(doc.getFieldValue(configuration.getPathField()));
                            // if path doesn't exist in the node state, filter the facets
                            PermissionProvider permissionProvider = filter.getSelector().getQuery().getExecutionContext().getPermissionProvider();
                            for (FacetField ff : facetFields) {
                                if (permissionProvider != null && !permissionProvider.canRead(path, ff.getName())) {
                                    filterFacet(doc, ff);
                                }
                            }
//                            if (!exists(path, root) && facetFields.size() > 0) {
//                                filterFacets(doc, facetFields);
//                            }
                        }

                        onRetrievedResults(filter, docs);

                        if (log.isDebugEnabled()) {
                            log.debug("getting docs {}", docs);
                        }

                        for (SolrDocument doc : docs) {
                            SolrResultRow row = convertToRow(doc);
                            if (row != null) {
                                queue.add(row);
                            }
                            lastDocToRecord = doc;
                        }
                    } catch (Exception e) {
                        if (log.isWarnEnabled()) {
                            log.warn("query via {} failed.", solrServer, e);
                        }
                    }
                    if (lastDocToRecord != null) {
                        this.lastDoc = lastDocToRecord;
                    }

                    return !queue.isEmpty();
                }

            }, filter.getQueryEngineSettings());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return cursor;
    }

    private void filterFacet(SolrDocument doc, FacetField facetField) {
        if (doc.getFieldNames().contains(facetField.getName())) {
            // decrease facet value
            Collection<Object> docFieldValues = doc.getFieldValues(facetField.getName());
            if (docFieldValues != null) {
                for (Object docFieldValue : docFieldValues) {
                    String valueString = String.valueOf(docFieldValue);
                    for (FacetField.Count count : facetField.getValues()) {
                        if (valueString.equals(count.getName())) {
                            count.setCount(count.getCount() - 1);
                        }
                    }
                }
            }
        }
    }


    void onRetrievedResults(Filter filter, SolrDocumentList docs) {
        // do nothing
    }

    private void filterFacets(SolrDocument doc, List<FacetField> facetFields) {
        // TODO : facet filtering by value requires that the facet values match the stored values
        // TODO : a *_facet field must exist, storing docValues instead of values and that should be used for faceting and at filtering time
        for (FacetField facetField : facetFields) {
            if (doc.getFieldNames().contains(facetField.getName())) {
                // decrease facet value
                Collection<Object> docFieldValues = doc.getFieldValues(facetField.getName());
                if (docFieldValues != null) {
                    for (Object docFieldValue : docFieldValues) {
                        String valueString = String.valueOf(docFieldValue);
                        for (FacetField.Count count : facetField.getValues()) {
                            if (valueString.equals(count.getName())) {
                                count.setCount(count.getCount() - 1);
                            }
                        }
                    }
                }
            }
        }
    }

    private boolean exists(String path, NodeState root) {
        // need to enable the check at the property level too
        boolean nodeExists = true;
        NodeState nodeState = root;
        for (String n : PathUtils.elements(path)) {
            if (nodeState.hasChildNode(n)) {
                nodeState = nodeState.getChildNode(n);
            } else {
                nodeExists = false;
                break;
            }
        }
        return nodeExists;
    }

    static boolean isIgnoredProperty(String propertyName, OakSolrConfiguration configuration) {
        return !configuration.useForPropertyRestrictions() // Solr index not used for properties
                || (configuration.getUsedProperties().size() > 0 && !configuration.getUsedProperties().contains(propertyName)) // not explicitly contained in the used properties
                || propertyName.contains("/") // no child-level property restrictions
                || "rep:excerpt".equals(propertyName) // rep:excerpt is handled by the query engine
                || configuration.getIgnoredProperties().contains(propertyName); // property is explicitly ignored
    }

    static class SolrResultRow {
        String path;
        double score;
        SolrDocument doc;
        List<FacetField> facetFields;

        SolrResultRow(String path, double score) {
            this(path, score, null);
        }

        SolrResultRow(String path, double score, SolrDocument doc) {
            this.path = path;
            this.score = score;
            this.doc = doc;
        }

        SolrResultRow(String path, double score, SolrDocument doc, List<FacetField> facetFields) {
            this.path = path;
            this.score = score;
            this.doc = doc;
            this.facetFields = facetFields;
        }

        @Override
        public String toString() {
            return String.format("%s (%1.2f)", path, score);
        }
    }

    /**
     * A cursor over Solr results. The result includes the path and the jcr:score pseudo-property as returned by Solr,
     * plus, eventually, the returned stored values if {@link org.apache.solr.common.SolrDocument} is included in the
     * {@link org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex.SolrResultRow}.
     */
    static class SolrRowCursor implements Cursor {

        private final Cursor pathCursor;
        SolrResultRow currentRow;

        SolrRowCursor(final Iterator<SolrResultRow> it, QueryEngineSettings settings) {

            Iterator<String> pathIterator = new Iterator<String>() {

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public String next() {
                    currentRow = it.next();
                    return currentRow.path;
                }

                @Override
                public void remove() {
                    it.remove();
                }

            };
            pathCursor = new Cursors.PathCursor(pathIterator, true, settings);

        }

        @Override
        public boolean hasNext() {
            return pathCursor.hasNext();
        }

        @Override
        public void remove() {
            pathCursor.remove();
        }

        @Override
        public IndexRow next() {
            final IndexRow pathRow = pathCursor.next();
            return new IndexRow() {

                @Override
                public String getPath() {
                    return pathRow.getPath();
                }

                @Override
                public PropertyValue getValue(String columnName) {
                    // overlay the score
                    if (QueryImpl.JCR_SCORE.equals(columnName)) {
                        return PropertyValues.newDouble(currentRow.score);
                    }
                    Matcher m = FilterQueryParser.FACET_REGEX.matcher(columnName);
                    if (m.matches()) { // facets
                        String facetFieldName = m.group(1);
                        FacetField facetField = null;
                        for (FacetField ff : currentRow.facetFields) {
                            if (ff.getName().equals(facetFieldName)) {
                                facetField = ff;
                                break;
                            }
                        }

                        if (facetField != null) {
                            return PropertyValues.newString(facetField.toString());
                        } else {
                            return null;
                        }
                    }
                    // TODO : make inclusion of doc configurable
                    return currentRow.doc != null ? PropertyValues.newString(
                            String.valueOf(currentRow.doc.getFieldValue(columnName))) : null;
                }

            };
        }
    }

    @Override
    @CheckForNull
    public NodeAggregator getNodeAggregator() {
        return aggregator;
    }

}
