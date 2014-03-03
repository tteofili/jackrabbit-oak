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
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi service for {@link org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider}
 */
@Component(metatype = true, label = "SolrServer provider", immediate = true)
@Service(SolrServerProviderFactory.class)
public class SolrServerProviderFactoryService implements SolrServerProviderFactory {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Property(options = {
            @PropertyOption(name = "embedded",
                    value = "Embedded Solr"
            ),
            @PropertyOption(name = "remote",
                    value = "Remote Solr"
            )},
            value = "remote"
    )
    private static final String SERVER_TYPE = "server.type";

    private BundleContext bundleContext;

    private SolrServerProvider solrServerProvider;
    private String serverType;

    @Activate
    protected void activate(ComponentContext context) throws Exception {
        serverType = String.valueOf(context.getProperties().get(SERVER_TYPE));
        bundleContext = context.getBundleContext();
    }

    @Override
    public SolrServerProvider getSolrServerProvider() {
        if (solrServerProvider == null) {
            log.info("getting server provider");
            try {
                ServiceReference[] serviceReferences = bundleContext.getServiceReferences(SolrServerConfigurationProvider.class.getName(), null);
                log.info("found references {}", serviceReferences != null ? serviceReferences.length : false);
                if (serviceReferences != null) {
                    for (ServiceReference serviceReference : serviceReferences) {
                        Object name = serviceReference.getProperty("name");
                        log.info("name : "+name);
                        if (serverType.equals(name)) {
                            SolrServerConfigurationProvider solrServerConfigurationProvider = (SolrServerConfigurationProvider) bundleContext.getService(serviceReference);
                            log.info("conf provider {}", solrServerConfigurationProvider);
                            SolrServerConfiguration solrServerConfiguration = solrServerConfigurationProvider.getSolrServerConfiguration();
                            log.info("server conf {}", solrServerConfiguration);
                            solrServerProvider = solrServerConfiguration.newInstance();
                            log.info("server provider {}", solrServerProvider);
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                // do nothing
                log.error("error while getting solr server {}", e);
            }
        }
        return solrServerProvider;
    }
}
