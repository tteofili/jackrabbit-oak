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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.AbstractAnalysisFactory;
import org.apache.lucene.util.Version;

/**
 * Constants used internally in Lucene indexes.
 */
public interface LuceneIndexConstants {

    String TYPE_LUCENE = "lucene";

    String SUGGEST_DATA_CHILD_NAME = ":suggest-data";

    String TRASH_CHILD_NAME = ":trash";

    Version VERSION = Version.LUCENE_47;

    Analyzer ANALYZER = new OakAnalyzer(VERSION);

    /**
     * Name of the codec to be used for indexing
     */
    String CODEC_NAME = "codec";

    /**
     * Name of the merge policy to be used while indexing
     */
    String MERGE_POLICY_NAME = "mergePolicy";

    /**
     * Boolean property to indicate that LuceneIndex is being used in testMode
     * and it should participate in every test
     */
    String TEST_MODE = "testMode";

    /**
     * Boolean property indicating if in-built analyzer should preserve original term
     * (i.e. use
     * {@link org.apache.lucene.analysis.miscellaneous.WordDelimiterFilter#PRESERVE_ORIGINAL}
     * flag)
     */
    String INDEX_ORIGINAL_TERM = "indexOriginalTerm";

    /**
     * Node name under which various analyzers are configured
     */
    String ANALYZERS = "analyzers";

    /**
     * Name of the default analyzer definition node under 'analyzers' node
     */
    String ANL_DEFAULT = "default";
    String ANL_FILTERS = "filters";
    String ANL_STOPWORDS = "stopwords";
    String ANL_TOKENIZER = "tokenizer";
    String ANL_CHAR_FILTERS = "charFilters";
    String ANL_CLASS = "class";
    String ANL_NAME = "name";
    String ANL_LUCENE_MATCH_VERSION = AbstractAnalysisFactory.LUCENE_MATCH_VERSION_PARAM;

    /**
     * Boolean property indicating that Lucene directory content
     * should be saved as part of NodeState itself as a multi value property
     * to allow faster reads (OAK-2809)
     */
    String SAVE_DIR_LISTING = "saveDirectoryListing";

    /**
     * Optional  Property to store the path of index in the repository. Path at which index
     * definition is defined is not known to IndexEditor. To make use of CopyOnWrite
     * feature its required to know the indexPath to optimize the lookup and read of
     * existing index files
     *
     * @deprecated With OAK-4152 no need to explicitly define indexPath property
     */
    @Deprecated
    String INDEX_PATH = "indexPath";

}
