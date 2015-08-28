/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.jackrabbit.oak.scalability.suites;

import java.io.File;
import javax.annotation.Nonnull;
import javax.jcr.Repository;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.DefaultSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.EmbeddedSolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.index.SolrIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.EmbeddedSolrServerProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.util.SolrIndexInitializer;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.solr.client.solrj.SolrServer;

import com.google.common.base.Strings;

/**
 * The suite test will incrementally increase the load and execute searches, with only the Solr index loaded.
 * Each test run thus adds nodes and executes different benchmarks. This way we measure time taken for
 * benchmark execution.
 * <p/>
 * {# NODE_LEVELS} is a comma separated string property and governs the depth and the number of
 * nodes in the hierarchy.
 */
public class SolrScalabilityNodeSearchSuite extends ScalabilityNodeSuite {


    private SolrServerProvider solrServerProvider;


    public SolrScalabilityNodeSearchSuite(Boolean storageEnabled) {
        super(storageEnabled);
    }

    @Override
    protected void afterSuite() throws Exception {
        SolrServer solrServer = solrServerProvider.getSolrServer();
        if (solrServer != null) {
            solrServer.shutdown();
        }
        solrServer = solrServerProvider.getIndexingSolrServer();
        if (solrServer != null) {
            solrServer.shutdown();
        }
        super.afterSuite();
    }


    private EmbeddedSolrServerProvider createEmbeddedSolrServerProvider(boolean http) {
        String tempDirectoryPath = FileUtils.getTempDirectoryPath();
        File solrHome = new File(tempDirectoryPath, "solr" + System.nanoTime());
        EmbeddedSolrServerConfiguration embeddedSolrServerConfiguration = new EmbeddedSolrServerConfiguration(solrHome.getAbsolutePath(), "oak");
        if (http) {
            embeddedSolrServerConfiguration = embeddedSolrServerConfiguration.withHttpConfiguration("/solr", 8983);
        }
        EmbeddedSolrServerProvider embeddedSolrServerProvider = null;
        try {
            embeddedSolrServerProvider = embeddedSolrServerConfiguration.getProvider();
            embeddedSolrServerProvider.getSolrServer(); // start the server
            return embeddedSolrServerProvider;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    LuceneIndexProvider provider = new LuceneIndexProvider();
                    OakSolrConfigurationProvider configurationProvider = new OakSolrConfigurationProvider() {
                        @Nonnull
                        @Override
                        public OakSolrConfiguration getConfiguration() {
                            return new DefaultSolrConfiguration() {
                                @Override
                                public boolean useForPropertyRestrictions() {
                                    return true;
                                }
                            };
                        }
                    };
                    solrServerProvider = createEmbeddedSolrServerProvider(false);
                    oak.with((QueryIndexProvider) provider)
                            .with((Observer) provider)
                            .with(new SolrIndexEditorProvider(solrServerProvider, configurationProvider))
                            .with(new SolrQueryIndexProvider(solrServerProvider, configurationProvider))
                            .with(new LuceneIndexEditorProvider());

                    if (!Strings.isNullOrEmpty(ASYNC_INDEX) && ASYNC_INDEX
                            .equals(IndexConstants.ASYNC_PROPERTY_NAME)) {
                        oak.withAsyncIndexing();
                    }

                    if (FULL_TEXT) {
                        oak.with(new LuceneInitializerHelper("luceneGlobal", storageEnabled));
                    }
                    oak.with(new SolrIndexInitializer(false, "solr", true));

                    whiteboard = oak.getWhiteboard();
                    return new Jcr(oak);
                }
            });
        }
        return super.createRepository(fixture);
    }


}