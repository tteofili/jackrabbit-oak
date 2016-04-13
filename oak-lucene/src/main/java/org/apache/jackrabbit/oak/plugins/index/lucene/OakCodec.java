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
import org.apache.lucene.codecs.lucene50.Lucene50FieldInfosFormat;
import org.apache.lucene.codecs.lucene50.Lucene50LiveDocsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50SegmentInfoFormat;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50TermVectorsFormat;
import org.apache.lucene.codecs.lucene53.Lucene53NormsFormat;
import org.apache.lucene.codecs.lucene54.Lucene54Codec;
import org.apache.lucene.codecs.lucene54.Lucene54DocValuesFormat;

/**
 * Oak specific {@link org.apache.lucene.codecs.Codec}.
 *
 * It simply mimics {@link org.apache.lucene.codecs.lucene54.Lucene54Codec} but with uncompressed {@link org.apache.lucene.codecs.StoredFieldsFormat}.
 */
public class OakCodec extends FilterCodec {

    private final TermVectorsFormat vectorsFormat = new Lucene50TermVectorsFormat();
    private final FieldInfosFormat fieldInfosFormat = new Lucene50FieldInfosFormat();
    private final SegmentInfoFormat segmentInfosFormat = new Lucene50SegmentInfoFormat();
    private final LiveDocsFormat liveDocsFormat = new Lucene50LiveDocsFormat();
    private final PostingsFormat defaultFormat = new Lucene50PostingsFormat();
    private final DocValuesFormat defaultDVFormat = new Lucene54DocValuesFormat();
    private final NormsFormat normsFormat = new Lucene53NormsFormat();
    private final StoredFieldsFormat fieldsFormat = new Lucene50StoredFieldsFormat();

    public OakCodec() {
        super("oakCodec", new Lucene54Codec());
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
