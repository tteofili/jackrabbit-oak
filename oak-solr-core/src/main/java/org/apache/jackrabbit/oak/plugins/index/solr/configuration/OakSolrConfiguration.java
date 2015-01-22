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
package org.apache.jackrabbit.oak.plugins.index.solr.configuration;

import java.util.Collection;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.query.Filter;

/**
 * A Solr configuration holding all the possible customizable parameters that
 * can be leveraged for an Oak search index.
 */
public interface OakSolrConfiguration {

    /**
     * Provide a field name to be used for indexing / searching a certain {@link org.apache.jackrabbit.oak.api.Type}
     *
     * @param propertyType the {@link org.apache.jackrabbit.oak.api.Type} to be indexed / searched
     * @return the name of the Solr field to be used for the given {@link org.apache.jackrabbit.oak.api.Type}, or {@code null}
     * if no specific field has been configured to handle the given {@code Type}.
     */
    @CheckForNull
    String getFieldNameFor(Type<?> propertyType);

    /**
     * Provide the field name for indexing / searching paths
     *
     * @return the name of the Solr field to be used for indexing and searching on paths (exact matching).
     */
    @Nonnull
    String getPathField();

    /**
     * Provide a field name to search over for the given {@link org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction}
     *
     * @param pathRestriction the {@link org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction} used for filtering
     *                        search results or {@code null} if no specific field has been configured for it.
     * @return the name of the Solr field to be used for the given {@code PathRestriction}.
     */
    @CheckForNull
    String getFieldForPathRestriction(Filter.PathRestriction pathRestriction);

    /**
     * Provide a field name to search over for the given {@link org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction}
     *
     * @param propertyRestriction the {@link org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction} used for filtering search results
     * @return the name of the Solr field to be used for the given {@code PropertyRestriction} or {@code null} if no specific field
     * has been configured for it.
     */
    @CheckForNull
    String getFieldForPropertyRestriction(Filter.PropertyRestriction propertyRestriction);

    /**
     * Provide the commit policy to be used by a given {@link org.apache.solr.client.solrj.SolrServer}
     *
     * @return a {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.CommitPolicy}
     */
    @Nonnull
    CommitPolicy getCommitPolicy();

    /**
     * Provide a field name that is used as the default "catch all" field for searching over all the data
     *
     * @return the name of the Solr field to be used as "catch all" field, or {@code null} if no specific field
     * has been configured for it.
     */
    @CheckForNull
    String getCatchAllField();

    /**
     * Provide the number of documents (rows) to be fetched for each Solr query
     *
     * @return the number of rows to fetch
     */
    int getRows();

    /**
     * Define if the Solr index should be used to address property restrictions
     *
     * @return <code>true</code> if {@link org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex} should be used
     * for {@link org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction}s
     */
    boolean useForPropertyRestrictions();

    /**
     * Define if the Solr index should be used to filter by primary types
     *
     * @return <code>true</code> if {@link org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex} should be used
     * for filtering by primary types
     */
    boolean useForPrimaryTypes();

    /**
     * Define if the Solr index should be used to filter by path restrictions
     *
     * @return <code>true</code> if {@link org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex} should be used
     * for filtering by {@link org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction}s
     */
    boolean useForPathRestrictions();

    /**
     * Provide the names of the properties that should be neither indexed nor searched by the Solr index
     *
     * @return a {@link java.util.Collection} of property names for properties to be ignored
     */
    @Nonnull
    Collection<String> getIgnoredProperties();

    /**
     * Provide the depth of the nodes that should be ACL checked by the index at query time.
     * 0 means no ACL check, -1 means check all the nodes (not recommended, likely to be *very* slow)
     * recommendation is to set a value between 1 and 3.
     *
     * @return the depth of the nodes that should be ACL checked by the Solr index
     */
    int getACLCheckPathDepth();

    /**
     * Provide the names of the properties that should be indexed and searched by the Solr index
     *
     * @return a {@link java.util.Collection} of property names for properties to be ignored
     */
    @Nonnull
    Collection<String> getUsedProperties();
}
