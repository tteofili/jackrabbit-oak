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
package org.apache.jackrabbit.oak.plugins.index.solr.configuration;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate.NodeStateSolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate.OakSolrNodeStateConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.server.OakSolrServer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrServer;

import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkState;

public class SolrIndexNode {

    static SolrIndexNode open(String indexPath, NodeState root, NodeState defnNodeState)
            throws IOException {
        if (isPersistedConfiguration(defnNodeState)) {
            OakSolrConfiguration configuration = new OakSolrNodeStateConfiguration(defnNodeState);
            SolrServerConfigurationProvider configurationProvider = new NodeStateSolrServerConfigurationProvider(defnNodeState.getChildNode("server"));
            OakSolrServer solrServer = new OakSolrServer(configurationProvider);
            return new SolrIndexNode(PathUtils.getName(indexPath), configuration, solrServer);
        } else {
            return null;
        }

    }

    private final String name;
    private final OakSolrConfiguration oakSolrConfiguration;
    private final SolrServer oakSolrServer;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private boolean closed = false;

    SolrIndexNode(String name, OakSolrConfiguration oakSolrConfiguration, OakSolrServer oakSolrServer)
            throws IOException {
        this.name = name;
        this.oakSolrConfiguration = oakSolrConfiguration;
        this.oakSolrServer = oakSolrServer;
    }

    String getName() {
        return name;
    }

    public OakSolrConfiguration getOakSolrConfiguration() {
        return oakSolrConfiguration;
    }

    public SolrServer getSolrServer() {
        return oakSolrServer;
    }

    boolean acquire() {
        lock.readLock().lock();
        if (closed) {
            lock.readLock().unlock();
            return false;
        } else {
            return true;
        }
    }

    public void release() {
        lock.readLock().unlock();
    }

    void close() throws IOException {
        lock.writeLock().lock();
        try {
            checkState(!closed);
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }

        try {
            oakSolrServer.shutdown();
        } catch(Exception e) {
            // do nothing
        }
    }

    private static boolean isPersistedConfiguration(NodeState definition) {
        return definition.hasChildNode("server");
    }

}
