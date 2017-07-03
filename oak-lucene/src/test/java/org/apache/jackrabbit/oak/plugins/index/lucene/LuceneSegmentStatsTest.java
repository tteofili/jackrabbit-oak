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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.CopyOnReadDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.FileStoreStats;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for checking impacts of Lucene storage / configuration adjustments on the
 * {@link org.apache.jackrabbit.oak.segment.SegmentNodeStore}.
 */
@RunWith(Parameterized.class)
public class LuceneSegmentStatsTest extends AbstractQueryTest {

    private static final File DIRECTORY = new File("target/fs");

    private final boolean copyOnRW;
    private final String codec;

    private ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private String corDir = null;
    private String cowDir = null;

    private TestUtil.OptionalEditorProvider optionalEditorProvider = new TestUtil.OptionalEditorProvider();

    private FileStore fileStore;

    public LuceneSegmentStatsTest(boolean copyOnRW, String codec) {
        this.copyOnRW = copyOnRW;
        this.codec = codec;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {true, "oakCodec"},
                {false, "oakCodec"},
                {true, "customCodec"},
                {false, "customCodec"},
                {true, "Lucene46"},
                {false, "Lucene46"},
        });
    }

    @Before
    public void setUp() throws Exception {
        if (!DIRECTORY.exists()) {
            assert DIRECTORY.mkdirs();
        }
    }

    @After
    public void after() {
        new ExecutorCloser(executorService).close();
        IndexDefinition.setDisableStoredIndexDefinition(false);
        fileStore.close();
        if (DIRECTORY.exists()) {
            try {
                FileUtils.deleteDirectory(DIRECTORY);
            } catch (IOException e) {
                // do nothing
            }
        }
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    @Override
    protected ContentRepository createRepository() {
        LuceneIndexEditorProvider editorProvider;
        LuceneIndexProvider provider;
        if (copyOnRW) {
            IndexCopier copier = createIndexCopier();
            editorProvider = new LuceneIndexEditorProvider(copier, new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
            provider = new LuceneIndexProvider(copier);
        } else {
            editorProvider = new LuceneIndexEditorProvider();
            provider = new LuceneIndexProvider();
        }

        NodeStore nodeStore;
        try {
            fileStore = FileStoreBuilder.fileStoreBuilder(DIRECTORY).build();
            nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        } catch (IOException | InvalidFileStoreVersionException e) {
            throw new RuntimeException(e);
        }
        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(editorProvider)
                .with(optionalEditorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
    }

    private IndexCopier createIndexCopier() {
        try {
            return new IndexCopier(executorService, temporaryFolder.getRoot()) {
                @Override
                public Directory wrapForRead(String indexPath, IndexDefinition definition,
                                             Directory remote, String dirName) throws IOException {
                    Directory ret = super.wrapForRead(indexPath, definition, remote, dirName);
                    corDir = getFSDirPath(ret);
                    return ret;
                }

                @Override
                public Directory wrapForWrite(IndexDefinition definition,
                                              Directory remote, boolean reindexMode, String dirName) throws IOException {
                    Directory ret = super.wrapForWrite(definition, remote, reindexMode, dirName);
                    cowDir = getFSDirPath(ret);
                    return ret;
                }

                private String getFSDirPath(Directory dir) {
                    if (dir instanceof CopyOnReadDirectory) {
                        dir = ((CopyOnReadDirectory) dir).getLocal();
                    }

                    dir = unwrap(dir);

                    if (dir instanceof FSDirectory) {
                        return ((FSDirectory) dir).getDirectory().getAbsolutePath();
                    }
                    return null;
                }

                private Directory unwrap(Directory dir) {
                    if (dir instanceof FilterDirectory) {
                        return unwrap(((FilterDirectory) dir).getDelegate());
                    }
                    return dir;
                }

            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void shutdownExecutor() {
        executorService.shutdown();
    }

    @Test
    public void testLuceneIndexSegmentStats() throws Exception {
        IndexDefinitionBuilder idxb = new IndexDefinitionBuilder().noAsync().codec(codec);
        idxb.indexRule("nt:base").property("foo").analyzed().nodeScopeIndex().ordered().useInExcerpt().propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("lucenePropertyIndex");
        idxb.build(idx);

        Random r = new Random();
        Tree rootTree = root.getTree("/").addChild("content");
        byte[] bytes = new byte[10240];
        Charset charset = Charset.defaultCharset();
        for (int i = 0; i < 1000; i++) {
            r.nextBytes(bytes);
            String text = new String(bytes, charset);
            rootTree.addChild(String.valueOf(i)).setProperty("foo", text);
        }
        root.commit();

        fileStore.flush();

        FileStoreStats stats = fileStore.getStats();
        stats.flushed();
        String fileStoreInfoAsString = stats.fileStoreInfoAsString();
        System.out.println(codec + "," + copyOnRW);
        long directorySize = dumpFileStoreTo(new File("target/" + codec + "-" + copyOnRW));
        System.out.println(fileStoreInfoAsString + "\nDirectory size : " + directorySize);
    }

    private long dumpFileStoreTo(File to) throws IOException {
        if (!to.exists()) {
            assert to.mkdirs();
        }
        for (File f : DIRECTORY.listFiles()) {
            Files.copy(f, new File(to.getPath(), f.getName()));
        }

        long sizeOfDirectory = FileUtils.sizeOfDirectory(to);

        to.deleteOnExit();
        return sizeOfDirectory;
    }
}
