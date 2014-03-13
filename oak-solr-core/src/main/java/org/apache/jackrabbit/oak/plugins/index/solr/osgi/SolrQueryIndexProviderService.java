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

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Osgi Service that provides Solr based {@link org.apache.jackrabbit.oak.spi.query.QueryIndex}es
 *
 * @see org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndexProvider
 * @see QueryIndexProvider
 */
@Component(metatype = false, immediate = true)
@Service(value = QueryIndexProvider.class)
public class SolrQueryIndexProviderService implements QueryIndexProvider {

    @Reference
    private SolrServerProvider solrServerProvider;

    @Reference
    private OakSolrConfigurationProvider oakSolrConfigurationProvider;

    @Override
    @Nonnull
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        if (solrServerProvider != null && oakSolrConfigurationProvider != null) {
            return new SolrQueryIndexProvider(solrServerProvider,
                    oakSolrConfigurationProvider).getQueryIndexes(nodeState);
        } else {
            return new ArrayList<QueryIndex>();
        }
    }

}
