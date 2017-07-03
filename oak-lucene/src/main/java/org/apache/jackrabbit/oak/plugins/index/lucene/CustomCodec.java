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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.bloom.BloomFilteringPostingsFormat;
import org.apache.lucene.codecs.compressing.CompressingStoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.diskdv.DiskDocValuesFormat;
import org.apache.lucene.codecs.lucene40.Lucene40LiveDocsFormat;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat;
import org.apache.lucene.codecs.lucene41.Lucene41StoredFieldsFormat;
import org.apache.lucene.codecs.lucene42.Lucene42NormsFormat;
import org.apache.lucene.codecs.lucene42.Lucene42TermVectorsFormat;
import org.apache.lucene.codecs.lucene46.Lucene46Codec;
import org.apache.lucene.codecs.lucene46.Lucene46FieldInfosFormat;
import org.apache.lucene.codecs.lucene46.Lucene46SegmentInfoFormat;
import org.apache.lucene.codecs.memory.FSTPulsing41PostingsFormat;
import org.apache.lucene.codecs.pulsing.Pulsing41PostingsFormat;

/**
 * Custom codec to perform benchmark tests
 */
public class CustomCodec extends FilterCodec {

    private final TermVectorsFormat vectorsFormat = new Lucene42TermVectorsFormat();
    private final FieldInfosFormat fieldInfosFormat = new Lucene46FieldInfosFormat();
    private final SegmentInfoFormat segmentInfosFormat = new Lucene46SegmentInfoFormat();
    private final LiveDocsFormat liveDocsFormat = new Lucene40LiveDocsFormat();
    private final PostingsFormat defaultFormat = new Pulsing41PostingsFormat();
    private final DocValuesFormat defaultDVFormat = DocValuesFormat.forName("Lucene45");
    private final NormsFormat normsFormat = new Lucene42NormsFormat();
    private final StoredFieldsFormat fieldsFormat = new CompressingStoredFieldsFormat("Lucene41", CompressionMode.HIGH_COMPRESSION, 1024);

    public CustomCodec() {
        super("customCodec", new Lucene46Codec());
    }

    @Override
    public PostingsFormat postingsFormat() {
        return defaultFormat;
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return defaultDVFormat;
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return fieldsFormat;
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return vectorsFormat;
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return segmentInfosFormat;
    }

    @Override
    public NormsFormat normsFormat() {
        return normsFormat;
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        return liveDocsFormat;
    }
}
