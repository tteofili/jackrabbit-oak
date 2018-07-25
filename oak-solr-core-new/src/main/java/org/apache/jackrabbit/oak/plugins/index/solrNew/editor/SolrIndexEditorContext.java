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
package org.apache.jackrabbit.oak.plugins.index.solrNew.editor;

import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.Builder;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditorContext;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.solrNew.SolrIndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.common.SolrInputDocument;

public class SolrIndexEditorContext extends FulltextIndexEditorContext<SolrInputDocument> {

    protected SolrIndexEditorContext(NodeState root,
            NodeBuilder definition,
            IndexDefinition indexDefinition,
            IndexUpdateCallback updateCallback,
            FulltextIndexWriterFactory indexWriterFactory,
            ExtractedTextCache extractedTextCache,
            IndexingContext indexingContext, boolean asyncIndexing) {
        super(root, definition,
                indexDefinition,
                updateCallback,
                indexWriterFactory,
                extractedTextCache,
                indexingContext,
                asyncIndexing);
    }

    @Override
    public SolrDocumentMaker newDocumentMaker(IndexingRule rule, String path) {
        return new SolrDocumentMaker(getTextExtractor(), getDefinition(), rule, path);
    }

    @Override
    public Builder newDefinitionBuilder() {
        return new SolrIndexDefinition.Builder();
    }

}
