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
package org.apache.jackrabbit.oak.plugins.segment;

import java.io.IOException;
import java.io.InputStream;
import java.util.Dictionary;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.mongo.MongoStore;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.apache.jackrabbit.oak.spi.whiteboard.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.osgi.service.component.ComponentContext;

import com.mongodb.Mongo;

import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

@Component(policy = ConfigurationPolicy.REQUIRE)
@Service(NodeStore.class)
public class SegmentNodeStoreService extends AbstractNodeStore {

    @Property(description="The unique name of this instance")
    public static final String NAME = "name";

    @Property(description="TarMK directory (if unset, use MongoDB)")
    public static final String DIRECTORY = "repository.home";

    @Property(description="MongoDB host")
    public static final String HOST = "host";

    @Property(description="MongoDB host", intValue=27017)
    public static final String PORT = "port";

    @Property(description="MongoDB database", value="Oak")
    public static final String DB = "db";

    @Property(description="Cache size (MB)", intValue=200)
    public static final String CACHE = "cache";

    private static final long MB = 1024 * 1024;

    private String name;

    private Mongo mongo;

    private SegmentStore store;

    private NodeStore delegate;

    private Registration cacheStatsReg;

    private synchronized NodeStore getDelegate() {
        assert delegate != null : "service must be activated when used";
        return delegate;
    }

    @Activate
    public synchronized void activate(ComponentContext context)
            throws IOException {
        Dictionary<?, ?> properties = context.getProperties();
        name = "" + properties.get(NAME);

        String host = lookup(context, HOST);
        if (host == null) {
            String directory = lookup(context, DIRECTORY);
            mongo = null;
            store = new FileStore(directory);
        } else {
            int port = Integer.parseInt(String.valueOf(properties.get(PORT)));
            String db = String.valueOf(properties.get(DB));
            int cache = Integer.parseInt(String.valueOf(properties.get(CACHE)));

            mongo = new Mongo(host, port);
            SegmentCache sc = new SegmentCache(cache * MB);
            store = new MongoStore(mongo.getDB(db), sc);

            cacheStatsReg = registerMBean(new OsgiWhiteboard(context.getBundleContext()), CacheStatsMBean.class,
                    sc.getCacheStats(), CacheStatsMBean.TYPE, sc.getCacheStats().getName());
        }

        delegate = new SegmentNodeStore(store);
    }

    private static String lookup(ComponentContext context, String property) {
        if (context.getProperties().get(property) != null) {
            return context.getProperties().get(property).toString();
        }
        if (context.getBundleContext().getProperty(property) != null) {
            return context.getBundleContext().getProperty(property).toString();
        }
        return null;
    }

    @Deactivate
    public synchronized void deactivate() {
        delegate = null;

        if(cacheStatsReg != null){
            cacheStatsReg.unregister();
        }

        store.close();
        if (mongo != null) {
            mongo.close();
        }
    }

    //---------------------------------------------------------< NodeStore >--

    @Override @Nonnull
    public NodeState getRoot() {
        return getDelegate().getRoot();
    }

    @Override @Nonnull
    public NodeStoreBranch branch() {
        return getDelegate().branch();
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return getDelegate().createBlob(stream);
    }

    @Override @Nonnull
    public String checkpoint(long lifetime) {
        return getDelegate().checkpoint(lifetime);
    }

    @Override @CheckForNull
    public NodeState retrieve(@Nonnull String checkpoint) {
        return getDelegate().retrieve(checkpoint);
    }

    //------------------------------------------------------------< Object >--

    @Override
    public synchronized String toString() {
        return name + ": " + super.toString();
    }

}
