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
package org.apache.jackrabbit.oak.plugins.index.solr.util;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * A {@link org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer} for Solr index
 */
public class SolrIndexInitializer implements RepositoryInitializer {

    private static final String SOLR_IDX = "solr";

    @Override
    public void initialize(@Nonnull NodeBuilder builder) {
        if (builder.hasChildNode(IndexConstants.INDEX_DEFINITIONS_NAME)
                && !builder.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME).hasChildNode(SOLR_IDX)) {
            builder = builder.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME).child(SOLR_IDX);
            builder.setProperty(JcrConstants.JCR_PRIMARYTYPE, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE)
                   .setProperty(IndexConstants.TYPE_PROPERTY_NAME, "solr")
                   .setProperty(IndexConstants.REINDEX_PROPERTY_NAME, "true");

        }
    }
}
