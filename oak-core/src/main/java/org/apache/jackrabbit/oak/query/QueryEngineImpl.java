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

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import joshua.decoder.Decoder;
import joshua.decoder.JoshuaConfiguration;
import joshua.decoder.io.TranslationRequestStream;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.namepath.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.query.xpath.XPathToSQL2Converter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;

/**
 * The query engine implementation.
 */
public abstract class QueryEngineImpl implements QueryEngine {

    public static final String CONTAINS = "contains(., '";
    private static Decoder decoder;

    /**
     * Used to instruct the {@link QueryEngineImpl} on how to act with respect of the SQL2
     * optimisation.
     */
    public static enum QuerySelectionMode {

        /**
         * Will execute the cheapest (default).
         */
        CHEAPEST,

        /**
         * Will use the original SQL2 query.
         */
        ORIGINAL,

        /**
         * Will force the computed alternate query to be executed. If available.
         */
        ALTERNATIVE

    }

    private static final AtomicInteger ID_COUNTER = new AtomicInteger();
    private static final String MDC_QUERY_ID = "oak.query.id";
    private static final String OAK_QUERY_ANALYZE = "oak.query.analyze";

    static final String SQL2 = "JCR-SQL2";
    static final String SQL = "sql";
    static final String XPATH = "xpath";
    static final String JQOM = "JCR-JQOM";

    static final String NO_LITERALS = "-noLiterals";

    static final Logger LOG = LoggerFactory.getLogger(QueryEngineImpl.class);

    private static final Set<String> SUPPORTED_LANGUAGES = of(
            SQL2, SQL2 + NO_LITERALS,
            SQL, SQL + NO_LITERALS,
            XPATH, XPATH + NO_LITERALS,
            JQOM);

    /**
     * Whether node traversal is enabled. This is enabled by default, and can be
     * disabled for testing purposes.
     */
    private boolean traversalEnabled = true;

    /**
     * Which query to select in case multiple options are available. Whether the
     * query engine should pick the one with the lowest expected cost (default),
     * or the original, or the alternative.
     */
    private QuerySelectionMode querySelectionMode = QuerySelectionMode.CHEAPEST;

    /**
     * Get the execution context for a single query execution.
     *
     * @return the context
     */
    protected abstract ExecutionContext getExecutionContext();

    @Override
    public Set<String> getSupportedQueryLanguages() {
        return SUPPORTED_LANGUAGES;
    }

    /**
     * Parse the query (check if it's valid) and get the list of bind variable names.
     *
     * @param statement query statement
     * @param language query language
     * @param mappings namespace prefix mappings
     * @return the list of bind variable names
     * @throws ParseException
     */
    @Override
    public List<String> getBindVariableNames(
            String statement, String language, Map<String, String> mappings)
            throws ParseException {
        List<Query> qs = parseQuery(statement, language, getExecutionContext(), mappings);

        return qs.iterator().next().getBindVariableNames();
    }

    /**
     * Parse the query.
     *
     * @param statement the statement
     * @param language the language
     * @param context the context
     * @param mappings the mappings
     * @return the list of queries, where the first is the original, and all
     *         others are alternatives (for example, a "union" query)
     */
    private static List<Query> parseQuery(
            String statement, String language, ExecutionContext context,
            Map<String, String> mappings) throws ParseException {

        boolean isInternal = SQL2Parser.isInternal(statement);
        if (isInternal) {
            LOG.trace("Parsing {} statement: {}", language, statement);
        } else {
            LOG.debug("Parsing {} statement: {}", language, statement);
        }

        NamePathMapper mapper = new NamePathMapperImpl(
                new LocalNameMapper(context.getRoot(), mappings));

        NodeState types = context.getBaseState()
                .getChildNode(JCR_SYSTEM)
                .getChildNode(JCR_NODE_TYPES);
        QueryEngineSettings settings = context.getSettings();

        SQL2Parser parser = new SQL2Parser(mapper, types, settings);
        if (language.endsWith(NO_LITERALS)) {
            language = language.substring(0, language.length() - NO_LITERALS.length());
            parser.setAllowNumberLiterals(false);
            parser.setAllowTextLiterals(false);
        }

        ArrayList<Query> queries = new ArrayList<Query>();

        Query q;

        if (SQL2.equals(language) || JQOM.equals(language)) {
            q = parser.parse(statement, false);
        } else if (SQL.equals(language)) {
            parser.setSupportSQL1(true);
            q = parser.parse(statement, false);
        } else if (XPATH.equals(language)) {
            XPathToSQL2Converter converter = new XPathToSQL2Converter();
            String sql2 = converter.convert(statement);
            LOG.debug("XPath > SQL2: {}", sql2);
            try {
                // OAK-874: No artificial XPath selector name in wildcards
                parser.setIncludeSelectorNameInWildcardColumns(false);
                q = parser.parse(sql2, false);
            } catch (ParseException e) {
                ParseException e2 = new ParseException(
                        statement + " converted to SQL-2 " + e.getMessage(), 0);
                e2.initCause(e);
                throw e2;
            }
        } else {
            throw new ParseException("Unsupported language: " + language, 0);
        }

        queries.add(q);

        // eventually translate the query
        String statement1 = q.getStatement();
        int i = statement1.indexOf(CONTAINS);
        if (i > 0) {
            int start = i + CONTAINS.length();
            int end = statement1.indexOf("')", start);
            String text = statement1.substring(start, end);
            // translate text
            try {
                Decoder decoder = getDecoder();
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                decoder.decodeAll(new TranslationRequestStream(new BufferedReader(new StringReader(text)), decoder.getJoshuaConfiguration()), os);
                os.flush();
                byte[] bytes = os.toByteArray();
                String translationOutput = IOUtils.toString(bytes, Charset.defaultCharset().name());
                String translatedTerm = parseTranslation(translationOutput);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (settings.isSql2Optimisation()) {
            if (q.isInternal()) {
                LOG.trace("Skipping optimisation as internal query.");
            } else {
                LOG.trace("Attempting optimisation");
                Query q2 = q.buildAlternativeQuery();
                if (q2 != q) {
                    LOG.debug("Alternative query available: {}", q2);
                    queries.add(q2);
                }
            }
        }

        // initialising all the queries.
        for (Query query : queries) {
            try {
                query.init();
            } catch (Exception e) {
                ParseException e2 = new ParseException(query.getStatement() + ": " + e.getMessage(), 0);
                e2.initCause(e);
                throw e2;
            }
        }

        return queries;
    }

    public static String parseTranslation(String output) {
        // format is lines of
        // 0 ||| hello ? ||| WordPenalty=-1.737 lm_0=-9.614 tm_glue_0=1.000
        // tm_pt_0=-4.154 tm_pt_1=-1.524 tm_pt_2=-1.000 tm_pt_4=-2.813
        // tm_pt_5=-1.410 ||| 6.737

        if(output == null || (output.equals(""))){
            return null;
        }

        String lines[] = output.split("\\r?\\n");
        String[] toks = lines[0].split("\\|\\|\\|");
        return toks[1];
    }



    private static Decoder getDecoder() {
        if (decoder == null) {
            String configFile = "/path/to/config";
            JoshuaConfiguration joshuaConfiguration = new JoshuaConfiguration();
            try {
                joshuaConfiguration.readConfigFile(configFile);
                decoder = new Decoder(joshuaConfiguration, configFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return decoder;
    }

    @Override
    public Result executeQuery(
            String statement, String language,
            Map<String, ? extends PropertyValue> bindings,
            Map<String, String> mappings) throws ParseException {
        return executeQuery(statement, language, Long.MAX_VALUE, 0, bindings, mappings);
    }

    @Override
    public Result executeQuery(
            String statement, String language, long limit, long offset,
            Map<String, ? extends PropertyValue> bindings,
            Map<String, String> mappings) throws ParseException {
        if (limit < 0) {
            throw new IllegalArgumentException("Limit may not be negative, is: " + limit);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset may not be negative, is: " + offset);
        }

        // avoid having to deal with null arguments
        if (bindings == null) {
            bindings = NO_BINDINGS;
        }
        if (mappings == null) {
            mappings = NO_MAPPINGS;
        }

        ExecutionContext context = getExecutionContext();
        List<Query> queries = parseQuery(statement, language, context, mappings);

        for (Query q : queries) {
            q.setExecutionContext(context);
            q.setLimit(limit);
            q.setOffset(offset);
            if (bindings != null) {
                for (Entry<String, ? extends PropertyValue> e : bindings.entrySet()) {
                    q.bindValue(e.getKey(), e.getValue());
                }
            }
            q.setTraversalEnabled(traversalEnabled);
        }

        boolean mdc = false;
        try {
            Query query = prepareAndSelect(queries);
            mdc = setupMDC(query);
            return query.executeQuery();
        } finally {
            if (mdc) {
                clearMDC();
            }
        }
    }

    /**
     * Prepare all the available queries and by based on the {@link QuerySelectionMode} flag return
     * the appropriate.
     *
     * @param queries the list of queries to be executed. Cannot be null.
     *      If there are multiple, the first one is the original, and the second the alternative.
     * @return the query
     */
    @Nonnull
    private Query prepareAndSelect(@Nonnull List<Query> queries) {
        Query result = null;

        if (checkNotNull(queries).size() == 1) {
            // we only have the original query so we prepare and return it.
            result = queries.iterator().next();
            result.prepare();
            LOG.debug("No alternatives found. Query: {}", result);
        } else {
            double bestCost = Double.POSITIVE_INFINITY;

            // Always prepare all of the queries and compute the cheapest as
            // it's the default behaviour. That way, we always log the cost and
            // can more easily analyze problems. The querySelectionMode flag can
            // be used to override the cheapest.
            for (Query q : checkNotNull(queries)) {
                q.prepare();
                double cost = q.getEstimatedCost();
                LOG.debug("cost: {} for query {}", cost, q);
                if (q.containsUnfilteredFullTextCondition()) {
                    LOG.debug("contains an unfiltered fulltext condition");
                    cost = Double.POSITIVE_INFINITY;
                }
                if (result == null || cost < bestCost) {
                    result = q;
                    bestCost = cost;
                }
            }

            switch (querySelectionMode) {
                case ORIGINAL:
                    LOG.debug("Forcing the original SQL2 query to be executed by flag");
                    result = queries.get(0);
                    break;

                case ALTERNATIVE:
                    LOG.debug("Forcing the alternative SQL2 query to be executed by flag");
                    result = queries.get(1);
                    break;

                // CHEAPEST is the default behaviour
                case CHEAPEST:
                default:
            }
        }

        return result;
    }

    protected void setTraversalEnabled(boolean traversalEnabled) {
        this.traversalEnabled = traversalEnabled;
    }

    private static boolean setupMDC(Query q) {
        boolean mdcEnabled = false;
        if (q.isMeasureOrExplainEnabled()) {
            MDC.put(OAK_QUERY_ANALYZE, Boolean.TRUE.toString());
            mdcEnabled = true;
        }

        if (LOG.isDebugEnabled()) {
            MDC.put(MDC_QUERY_ID, String.valueOf(ID_COUNTER.incrementAndGet()));
            mdcEnabled = true;
        }
        return mdcEnabled;
    }

    private static void clearMDC() {
        MDC.remove(MDC_QUERY_ID);
        MDC.remove(OAK_QUERY_ANALYZE);
    }

    /**
     * Instruct the query engine on how to behave with regards to the SQL2 optimised query if
     * available.
     *
     * @param querySelectionMode cannot be null
     */
    protected void setQuerySelectionMode(@Nonnull QuerySelectionMode querySelectionMode) {
        this.querySelectionMode = querySelectionMode;
    }
}
