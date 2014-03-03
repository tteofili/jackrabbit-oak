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
package org.apache.jackrabbit.oak.plugins.index.solr.osgi;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyOption;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.solr.client.solrj.SolrServer;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.component.ComponentContext;

/**
 * OSGi service for {@link org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider}
 */
@Component(metatype = true, label = "SolrServer provider")
@Service(SolrServerProvider.class)
public class SolrServerProviderService implements SolrServerProvider {

    @Property(options = {
            @PropertyOption(name = "EMBEDDED",
                    value = "Embedded Solr"
            ),
            @PropertyOption(name = "REMOTE",
                    value = "Remote Solr"
            )},
            value = "REMOTE"
    )
    private static final String SERVER_TYPE = "server.type";

    private SolrServerProvider solrServerProvider;

    @Activate
    protected void activate(ComponentContext context) throws Exception {
        String serverType = String.valueOf(context.getProperties().get(SERVER_TYPE));
        BundleContext bundleContext = context.getBundleContext();
        ServiceReference[] serviceReferences = bundleContext.getServiceReferences(SolrServerConfigurationProvider.class.getName(), "(name = " + serverType + ")");
        if (serviceReferences.length == 1) {
            SolrServerConfigurationProvider solrServerConfigurationProvider = (SolrServerConfigurationProvider) bundleContext.getService(serviceReferences[0]);
            SolrServerConfiguration solrServerConfiguration = solrServerConfigurationProvider.getSolrServerConfiguration();
            solrServerProvider = solrServerConfiguration.newInstance();
        } else {
            throw new RuntimeException("could not find Solr server of type" + serverType);
        }
    }

    @Override
    public SolrServer getSolrServer() throws Exception {
        return solrServerProvider.getSolrServer();
    }
}
