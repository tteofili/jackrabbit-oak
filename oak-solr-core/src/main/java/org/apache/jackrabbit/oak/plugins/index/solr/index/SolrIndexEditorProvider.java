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
package org.apache.jackrabbit.oak.plugins.index.solr.index;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Solr based {@link IndexEditorProvider}
 *
 * @see SolrIndexEditor
 *
 */
public class SolrIndexEditorProvider implements IndexEditorProvider {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private SolrServerProvider solrServerProvider;

    private OakSolrConfigurationProvider oakSolrConfigurationProvider;

    public SolrIndexEditorProvider(
            SolrServerProvider solrServerProvider,
            OakSolrConfigurationProvider oakSolrConfigurationProvider) {
        this.solrServerProvider = solrServerProvider;
        this.oakSolrConfigurationProvider = oakSolrConfigurationProvider;
    }

    @Override
    public Editor getIndexEditor(
            String type, NodeBuilder definition, NodeState root)
            throws CommitFailedException {

        if (SolrQueryIndex.TYPE.equals(type)
                && isConfigurationOk()) {
            try {
                return new SolrIndexEditor(
                        definition,
                        solrServerProvider.getSolrServer(),
                        oakSolrConfigurationProvider.getConfiguration());
            } catch (Exception e) {
                if (log.isErrorEnabled()) {
                    log.error("unable to create SolrIndexEditor", e);
                }
            }
        }
        return null;
    }

    private boolean isConfigurationOk() {
        return solrServerProvider != null && oakSolrConfigurationProvider != null;
    }

}
