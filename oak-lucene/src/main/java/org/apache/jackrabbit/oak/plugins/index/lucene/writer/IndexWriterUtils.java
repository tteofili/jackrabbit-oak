/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.lucene.writer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.SuggestHelper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.Directory;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;

public class IndexWriterUtils {

    private static final Map<String, State> states = new HashMap<>();

    public static IndexWriterConfig getIndexWriterConfig(IndexDefinition definition, boolean remoteDir, Directory directory) {

        // FIXME: Hack needed to make Lucene work in an OSGi environment
        Thread thread = Thread.currentThread();
        ClassLoader loader = thread.getContextClassLoader();
        thread.setContextClassLoader(IndexWriterConfig.class.getClassLoader());
        try {
            Analyzer definitionAnalyzer = definition.getAnalyzer();
            Map<String, Analyzer> analyzers = new HashMap<String, Analyzer>();
            analyzers.put(FieldNames.SPELLCHECK, new ShingleAnalyzerWrapper(LuceneIndexConstants.ANALYZER, 3));
            if (!definition.isSuggestAnalyzed()) {
                analyzers.put(FieldNames.SUGGEST, SuggestHelper.getAnalyzer());
            }
            Analyzer analyzer = new PerFieldAnalyzerWrapper(definitionAnalyzer, analyzers);
            IndexWriterConfig config = new IndexWriterConfig(VERSION, analyzer);
            MergePolicy mergePolicy;
            try {
                IndexReader reader = DirectoryReader.open(directory);

                State previousState = states.get(definition.getIndexName());
                if (previousState == null) {
                    previousState = new State();
                }
                State newState = new State(reader.maxDoc(), reader.numDocs(), reader.numDeletedDocs(), directory.listAll());

                System.err.println("****");
                System.err.println(previousState + "\n vs \n" + newState);
                System.err.println("****");

                if (maybeMerge(previousState, newState)) {
                    mergePolicy = new OakTieredMergePolicy();

                } else {
                    mergePolicy = NoMergePolicy.COMPOUND_FILES;
                }
                states.put(definition.getIndexName(), newState);
            } catch (IOException e) {
                mergePolicy = new OakTieredMergePolicy();
            }
            config.setMergePolicy(mergePolicy);
//            config.setRAMBufferSizeMB(24);
            if (remoteDir) {
                config.setMergeScheduler(new SerialMergeScheduler());
            }
            if (definition.getCodec() != null) {
                config.setCodec(definition.getCodec());
            }
            return config;
        } finally {
            thread.setContextClassLoader(loader);
        }
    }

    private static boolean maybeMerge(State previousState, State newState) {
        return previousState.equals(newState) || Math.abs(newState.delDocs - previousState.delDocs) > 200 ||
                Math.abs(newState.docNum - previousState.docNum) > 200;
    }

    private static class State {
        private final int maxDoc;
        private final int docNum;
        private final int delDocs;
        private final String[] files;

        public State(int maxDoc, int docNum, int delDocs, String[] files) {
            this.maxDoc = maxDoc;
            this.docNum = docNum;
            this.delDocs = delDocs;
            this.files = files;
        }

        public State() {
            this.maxDoc = 0;
            this.docNum = 0;
            this.delDocs = 0;
            this.files = new String[0];
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            State state = (State) o;

            if (maxDoc != state.maxDoc) return false;
            if (docNum != state.docNum) return false;
            if (delDocs != state.delDocs) return false;
            // Probably incorrect - comparing Object[] arrays with Arrays.equals
            return Arrays.equals(files, state.files);
        }

        @Override
        public int hashCode() {
            int result = maxDoc;
            result = 31 * result + docNum;
            result = 31 * result + delDocs;
            result = 31 * result + Arrays.hashCode(files);
            return result;
        }

        @Override
        public String toString() {
            return "State{" +
                    "maxDoc=" + maxDoc +
                    ", docNum=" + docNum +
                    ", delDocs=" + delDocs +
                    ", files=" + Arrays.toString(files) +
                    '}';
        }
    }
}
