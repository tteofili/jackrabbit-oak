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

import java.io.File;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.EmbeddedSolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationDefaults;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.EmbeddedSolrServerProvider;
import org.osgi.service.component.ComponentContext;

/**
 * An OSGi service {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider}
 */
@Component(metatype = true, immediate = true,
        label = "Oak Solr embedded server configuration")
@Service(value = SolrServerConfigurationProvider.class)
@Property(name = "name", value = "embedded", propertyPrivate = true)
public class EmbeddedSolrServerConfigurationProvider implements SolrServerConfigurationProvider<EmbeddedSolrServerProvider> {

    @Property(value = SolrServerConfigurationDefaults.SOLR_HOME_PATH, label = "Solr home directory")
    private static final String SOLR_HOME_PATH = "solr.home.path";

    @Property(value = SolrServerConfigurationDefaults.CORE_NAME, label = "Solr Core name")
    private static final String SOLR_CORE_NAME = "solr.core.name";

    @Property(value = SolrServerConfigurationDefaults.SOLR_CONFIG_PATH, label = "Path to specific Solr Core configuration")
    private static final String SOLR_CONFIG_FILE = "solr.config.path";

<<<<<<< HEAD:oak-solr-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/solr/osgi/EmbeddedSolrServerConfigurationProvider.java
    @Property(value = SolrServerConfigurationDefaults.HTTP_PORT)
    private static final String SOLR_HTTP_PORT = "solr.http.port";

    @Property(value = SolrServerConfigurationDefaults.CONTEXT)
    private static final String SOLR_CONTEXT = "solr.context";

=======
>>>>>>> 946101f1867fca376a421a5346a53559d0aaf516:oak-solr-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/solr/osgi/EmbeddedSolrServerConfigurationProvider.java
    private String solrHome;
    private String solrConfigFile;
    private String solrCoreName;

<<<<<<< HEAD:oak-solr-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/solr/osgi/EmbeddedSolrServerConfigurationProvider.java
    private Integer solrHttpPort;
    private String solrContext;

=======
>>>>>>> 946101f1867fca376a421a5346a53559d0aaf516:oak-solr-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/solr/osgi/EmbeddedSolrServerConfigurationProvider.java
    private SolrServerConfiguration<EmbeddedSolrServerProvider> solrServerConfiguration;


    @Activate
    protected void activate(ComponentContext componentContext) throws Exception {
        solrHome = String.valueOf(componentContext.getProperties().get(SOLR_HOME_PATH));
        File file = new File(solrHome);
        if (!file.exists()) {
            assert file.createNewFile();
        }
        solrConfigFile = String.valueOf(componentContext.getProperties().get(SOLR_CONFIG_FILE));
        solrCoreName = String.valueOf(componentContext.getProperties().get(SOLR_CORE_NAME));

<<<<<<< HEAD:oak-solr-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/solr/osgi/EmbeddedSolrServerConfigurationProvider.java
        solrHttpPort = Integer.valueOf(String.valueOf(componentContext.getProperties().get(SOLR_HTTP_PORT)));
        solrContext = String.valueOf(componentContext.getProperties().get(SOLR_CONTEXT));

        solrServerConfiguration = new EmbeddedSolrServerConfiguration(solrHome, solrConfigFile, solrCoreName).
                withHttpConfiguration(solrContext, solrHttpPort);
=======
        solrServerConfiguration = new EmbeddedSolrServerConfiguration(solrHome, solrConfigFile, solrCoreName);
>>>>>>> 946101f1867fca376a421a5346a53559d0aaf516:oak-solr-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/solr/osgi/EmbeddedSolrServerConfigurationProvider.java
    }

    @Deactivate
    protected void deactivate() throws Exception {
        solrHome = null;
        solrConfigFile = null;
        solrCoreName = null;
<<<<<<< HEAD:oak-solr-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/solr/osgi/EmbeddedSolrServerConfigurationProvider.java
        solrContext = null;
=======
>>>>>>> 946101f1867fca376a421a5346a53559d0aaf516:oak-solr-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/solr/osgi/EmbeddedSolrServerConfigurationProvider.java
    }

    @Override
    public SolrServerConfiguration<EmbeddedSolrServerProvider> getSolrServerConfiguration() {
        return solrServerConfiguration;
    }
}
