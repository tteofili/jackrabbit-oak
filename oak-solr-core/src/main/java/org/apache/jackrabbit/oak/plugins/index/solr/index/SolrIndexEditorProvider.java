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

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.SolrServerProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that provides Lucene based {@link IndexEditor}s
 * 
 * @see SolrIndexEditor
 * @see IndexEditorProvider
 * 
 */
@Component
@Service(IndexEditorProvider.class)
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

    public SolrIndexEditorProvider() {
    }

    @Override
    public Editor getIndexEditor(
            String type, NodeBuilder definition, NodeState root)
            throws CommitFailedException {
        
        if (solrServerProvider == null) {
            BundleContext bundleContext = FrameworkUtil.getBundle(getClass()).getBundleContext();
            ServiceReference serverProviderServiceReference = bundleContext.getServiceReference(SolrServerProvider.class.getName());
            if (serverProviderServiceReference != null) {
                try {
                    solrServerProvider = (SolrServerProvider) bundleContext.getService(serverProviderServiceReference);
                } catch (Throwable t) {
                }
            }
        }

        if (oakSolrConfigurationProvider == null) {
            BundleContext bundleContext = FrameworkUtil.getBundle(getClass()).getBundleContext();
            ServiceReference configurationProviderServiceReference = bundleContext.getServiceReference(OakSolrConfigurationProvider.class.getName());
            if (configurationProviderServiceReference != null) {
                try {
                    oakSolrConfigurationProvider = (OakSolrConfigurationProvider) bundleContext.getService(configurationProviderServiceReference);
                } catch (Throwable t) {
                }
            }
        }

        if (SolrQueryIndex.TYPE.equals(type)
                && solrServerProvider != null
                && oakSolrConfigurationProvider != null) {
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

}
