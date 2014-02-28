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
package org.apache.jackrabbit.oak.plugins.index.solr.osgi.old;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.EmbeddedSolrServerProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.solr.client.solrj.SolrServer;
import org.osgi.service.component.ComponentContext;

/**
 * OSGi service for the embedded Solr server.
 */
@Component(metatype = true, label = "Embedded SolrServer provider")
@Service(value = {SolrServerProvider.class, OakSolrConfigurationProvider.class})
public class EmbeddedSolrProviderService implements SolrServerProvider {

    @Reference
    private SolrServerConfigurationProvider solrServerConfigurationProvider;

    @Reference
    private OakSolrConfigurationProvider oakSolrConfigurationProvider;

    private SolrServerProvider solrServerProvider;

    @Activate
    public void activate(ComponentContext context) throws Exception {
        solrServerProvider = new EmbeddedSolrServerProvider(
                solrServerConfigurationProvider.getSolrServerConfiguration());
    }

    @Override
    public SolrServer getSolrServer() throws Exception {
        return solrServerProvider.getSolrServer();
    }
}
