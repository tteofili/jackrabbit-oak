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

package org.apache.jackrabbit.oak.plugins.index.lucene.binary;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.io.CountingInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.io.LazyInputStream;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText;
import org.apache.jackrabbit.oak.plugins.index.lucene.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorContext;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Field;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.WriteOutContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newFulltextField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditor.TEXT_EXTRACTION_ERROR;

public class BinaryTextExtractor {
    private static final Logger log = LoggerFactory.getLogger(BinaryTextExtractor.class);
    private static final Parser defaultParser = createDefaultParser();
    private static final long SMALL_BINARY = Long.getLong("oak.lucene.smallBinary", 16 * 1024);
    private final TextExtractionStats textExtractionStats = new TextExtractionStats();
    private final ExtractedTextCache extractedTextCache;
    private final IndexDefinition definition;
    private final boolean reindex;
    private Parser parser;
    /**
     * The media types supported by the parser used.
     */
    private Set<MediaType> supportedMediaTypes;

    public BinaryTextExtractor(ExtractedTextCache extractedTextCache, IndexDefinition definition, boolean reindex) {
        this.extractedTextCache = extractedTextCache;
        this.definition = definition;
        this.reindex = reindex;
    }

    public void done(boolean reindex){
        textExtractionStats.log(reindex);
        textExtractionStats.collectStats(extractedTextCache);
    }

    public List<Field> newBinary(
            PropertyState property, NodeState state, String nodePath, String path) {
        List<Field> fields = new ArrayList<Field>();
        Metadata metadata = new Metadata();

        //jcr:mimeType is mandatory for a binary to be indexed
        String type = state.getString(JcrConstants.JCR_MIMETYPE);

        if (type == null || !isSupportedMediaType(type)) {
            log.trace(
                    "[{}] Ignoring binary content for node {} due to unsupported (or null) jcr:mimeType [{}]",
                    getIndexName(), path, type);
            return fields;
        }

        metadata.set(Metadata.CONTENT_TYPE, type);
        if (JCR_DATA.equals(property.getName())) {
            String encoding = state.getString(JcrConstants.JCR_ENCODING);
            if (encoding != null) { // not mandatory
                metadata.set(Metadata.CONTENT_ENCODING, encoding);
            }
        }

        for (Blob v : property.getValue(Type.BINARIES)) {
            String value = parseStringValue(v, metadata, path, property.getName());
            if (value == null){
                continue;
            }

            if (nodePath != null){
                fields.add(newFulltextField(nodePath, value, true));
            } else {
                fields.add(newFulltextField(value, true));
            }
        }
        return fields;
    }

    private String parseStringValue(Blob v, Metadata metadata, String path, String propertyName) {
        String text = extractedTextCache.get(path, propertyName, v, reindex);
        if (text == null){
            text = parseStringValue0(v, metadata, path);
        }
        return text;
    }

    private String parseStringValue0(Blob v, Metadata metadata, String path) {
        WriteOutContentHandler handler = new WriteOutContentHandler(definition.getMaxExtractLength());
        long start = System.currentTimeMillis();
        long bytesRead = 0;
        long length = v.length();
        if (log.isDebugEnabled()) {
            log.debug("Extracting {}, {} bytes, id {}", path, length, v.getContentIdentity());
        }
        String oldThreadName = null;
        if (length > SMALL_BINARY) {
            Thread t = Thread.currentThread();
            oldThreadName = t.getName();
            t.setName(oldThreadName + ": Extracting " + path + ", " + length + " bytes");
        }
        try {
            CountingInputStream stream = new CountingInputStream(new LazyInputStream(new BlobByteSource(v)));
            try {
                getParser().parse(stream, handler, metadata, new ParseContext());
            } finally {
                bytesRead = stream.getCount();
                stream.close();
            }
        } catch (LinkageError e) {
            // Capture and ignore errors caused by extraction libraries
            // not being present. This is equivalent to disabling
            // selected media types in configuration, so we can simply
            // ignore these errors.
        } catch (Throwable t) {
            // Capture and report any other full text extraction problems.
            // The special STOP exception is used for normal termination.
            if (!handler.isWriteLimitReached(t)) {
                log.debug(
                        "[{}] Failed to extract text from a binary property: {}."
                                + " This is a fairly common case, and nothing to"
                                + " worry about. The stack trace is included to"
                                + " help improve the text extraction feature.",
                        getIndexName(), path, t);
                extractedTextCache.put(v, ExtractedText.ERROR);
                return TEXT_EXTRACTION_ERROR;
            }
        } finally {
            if (oldThreadName != null) {
                Thread.currentThread().setName(oldThreadName);
            }
        }
        String result = handler.toString();
        if (bytesRead > 0) {
            long time = System.currentTimeMillis() - start;
            int len = result.length();
            recordTextExtractionStats(time, bytesRead, len);
            if (log.isDebugEnabled()) {
                log.debug("Extracting {} took {} ms, {} bytes read, {} text size",
                        path, time, bytesRead, len);
            }
        }
        extractedTextCache.put(v,  new ExtractedText(ExtractedText.ExtractionResult.SUCCESS, result));
        return result;
    }

    private void recordTextExtractionStats(long timeInMillis, long bytesRead, int textLength) {
        textExtractionStats.addStats(timeInMillis, bytesRead, textLength);
    }

    private String getIndexName() {
        return definition.getIndexName();
    }

    //~-------------------------------------------< Tika >

    private Parser getParser() {
        if (parser == null){
            parser = initializeTikaParser(definition);
        }
        return parser;
    }

    private boolean isSupportedMediaType(String type) {
        if (supportedMediaTypes == null) {
            supportedMediaTypes = getParser().getSupportedTypes(new ParseContext());
        }
        return supportedMediaTypes.contains(MediaType.parse(type));
    }

    private static Parser initializeTikaParser(IndexDefinition definition) {
        ClassLoader current = Thread.currentThread().getContextClassLoader();
        try {
            if (definition.hasCustomTikaConfig()) {
                log.debug("[{}] Using custom tika config", definition.getIndexName());
                Thread.currentThread().setContextClassLoader(LuceneIndexEditorContext.class.getClassLoader());
                InputStream is = definition.getTikaConfig();
                try {
                    return new AutoDetectParser(getTikaConfig(is, definition));
                } finally {
                    IOUtils.closeQuietly(is);
                }
            }
        }finally {
            Thread.currentThread().setContextClassLoader(current);
        }
        return defaultParser;
    }

    private static AutoDetectParser createDefaultParser() {
        ClassLoader current = Thread.currentThread().getContextClassLoader();
        URL configUrl = LuceneIndexEditorContext.class.getResource("tika-config.xml");
        InputStream is = null;
        if (configUrl != null) {
            try {
                Thread.currentThread().setContextClassLoader(LuceneIndexEditorContext.class.getClassLoader());
                is = configUrl.openStream();
                TikaConfig config = new TikaConfig(is);
                log.info("Loaded default Tika Config from classpath {}", configUrl);
                return new AutoDetectParser(config);
            } catch (Exception e) {
                log.warn("Tika configuration not available : " + configUrl, e);
            } finally {
                IOUtils.closeQuietly(is);
                Thread.currentThread().setContextClassLoader(current);
            }
        } else {
            log.warn("Default Tika configuration not found");
        }
        return new AutoDetectParser();
    }

    private static TikaConfig getTikaConfig(InputStream configStream, Object source){
        try {
            return new TikaConfig(configStream);
        } catch (Exception e) {
            log.warn("Tika configuration not available : "+source, e);
        }
        return TikaConfig.getDefaultConfig();
    }
}
