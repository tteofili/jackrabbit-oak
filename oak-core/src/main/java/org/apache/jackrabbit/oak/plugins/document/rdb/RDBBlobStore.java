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
package org.apache.jackrabbit.oak.plugins.document.rdb;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;

import javax.sql.DataSource;

import com.google.common.collect.AbstractIterator;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.spi.blob.AbstractBlobStore;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RDBBlobStore extends AbstractBlobStore implements Closeable {
    /**
     * Creates a {@linkplain RDBBlobStore} instance using an embedded H2
     * database in in-memory mode.
     */
    public RDBBlobStore() {
        try {
            String jdbcurl = "jdbc:h2:mem:oaknodes";
            Connection connection = DriverManager.getConnection(jdbcurl, "sa", "");
            initialize(connection);
        } catch (Exception ex) {
            throw new MicroKernelException("initializing RDB blob store", ex);
        }
    }

    /**
     * Creates a {@linkplain RDBBlobStore} instance using the provided JDBC
     * connection information.
     */
    public RDBBlobStore(String jdbcurl, String username, String password) {
        try {
            Connection connection = DriverManager.getConnection(jdbcurl, username, password);
            initialize(connection);
        } catch (Exception ex) {
            throw new MicroKernelException("initializing RDB blob store", ex);
        }
    }

    /**
     * Creates a {@linkplain RDBBlobStore} instance using the provided
     * {@link DataSource}.
     */
    public RDBBlobStore(DataSource ds) {
        try {
            initialize(ds.getConnection());
        } catch (Exception ex) {
            throw new MicroKernelException("initializing RDB blob store", ex);
        }
    }

    @Override
    public void close() {
        try {
            this.connection.close();
            this.connection = null;
        } catch (SQLException ex) {
            throw new MicroKernelException(ex);
        }
    }

    @Override
    public void finalize() {
        if (this.connection != null && this.callStack != null) {
            LOG.debug("finalizing RDBDocumentStore that was not disposed", this.callStack);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(RDBBlobStore.class);

    private Exception callStack;

    private Connection connection;

    private void initialize(Connection con) throws Exception {
        con.setAutoCommit(false);

        try {
            createTables(con, "binary");
        } catch (SQLException ex) {
            con.rollback();
            LOG.debug("failed to create tables, retrying after getting DB metadata", ex);
            // try to query database meta info for binary type
            String btype = RDBMeta.findBinaryType(con.getMetaData());
            if (btype == null) {
                String message = "Could not determine binary type for " + RDBMeta.getDataBaseName(con.getMetaData());
                LOG.error(message);
                throw new Exception(message);
            }
            createTables(con, btype);
        }

        this.connection = con;
        this.callStack = LOG.isDebugEnabled() ? new Exception("call stack of RDBBlobStore creation") : null;
    }

    private void createTables(Connection con, String binaryType) throws SQLException {
        Statement stmt = con.createStatement();
        stmt.execute("create table if not exists datastore_meta (id varchar primary key, level int, lastMod bigint)");
        stmt.execute("create table if not exists datastore_data (id varchar primary key, data " + binaryType + ")");
        stmt.close();
        con.commit();
    }

    private long minLastModified;

    @Override
    protected void storeBlock(byte[] digest, int level, byte[] data) throws IOException {
        try {
            storeBlockInDatabase(digest, level, data);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private void storeBlockInDatabase(byte[] digest, int level, byte[] data) throws SQLException {
        try {
            String id = StringUtils.convertBytesToHex(digest);
            long now = System.currentTimeMillis();
            PreparedStatement prep = connection.prepareStatement("update datastore_meta set lastMod = ? where id = ?");
            int count;
            try {
                prep.setLong(1, now);
                prep.setString(2, id);
                count = prep.executeUpdate();
            } finally {
                prep.close();
            }
            if (count == 0) {
                try {
                    prep = connection.prepareStatement("insert into datastore_data(id, data) values(?, ?)");
                    try {
                        prep.setString(1, id);
                        prep.setBytes(2, data);
                        prep.execute();
                    } finally {
                        prep.close();
                    }
                } catch (SQLException e) {
                    // already exists - ok
                }
                try {
                    prep = connection.prepareStatement("insert into datastore_meta(id, level, lastMod) values(?, ?, ?)");
                    try {
                        prep.setString(1, id);
                        prep.setInt(2, level);
                        prep.setLong(3, now);
                        prep.execute();
                    } finally {
                        prep.close();
                    }
                } catch (SQLException e) {
                    // already exists - ok
                }
            }
        } finally {
            connection.commit();
        }
    }

    @Override
    protected byte[] readBlockFromBackend(BlockId blockId) throws Exception {
        try {
            PreparedStatement prep = connection.prepareStatement("select data from datastore_data where id = ?");
            try {
                String id = StringUtils.convertBytesToHex(blockId.getDigest());
                prep.setString(1, id);
                ResultSet rs = prep.executeQuery();
                if (!rs.next()) {
                    throw new IOException("Datastore block " + id + " not found");
                }
                byte[] data = rs.getBytes(1);
                // System.out.println("    read block " + id + " blockLen: " +
                // data.length + " [0]: " + data[0]);
                if (blockId.getPos() == 0) {
                    return data;
                }
                int len = (int) (data.length - blockId.getPos());
                if (len < 0) {
                    return new byte[0];
                }
                byte[] d2 = new byte[len];
                System.arraycopy(data, (int) blockId.getPos(), d2, 0, len);
                return d2;
            } finally {
                prep.close();
            }
        } finally {
            connection.commit();
        }
    }

    @Override
    public void startMark() throws IOException {
        minLastModified = System.currentTimeMillis();
        markInUse();
    }

    @Override
    protected boolean isMarkEnabled() {
        return minLastModified != 0;
    }

    @Override
    protected void mark(BlockId blockId) throws Exception {
        try {
            if (minLastModified == 0) {
                return;
            }
            String id = StringUtils.convertBytesToHex(blockId.getDigest());
            PreparedStatement prep = connection
                    .prepareStatement("update datastore_meta set lastMod = ? where id = ? and lastMod < ?");
            prep.setLong(1, System.currentTimeMillis());
            prep.setString(2, id);
            prep.setLong(3, minLastModified);
            prep.executeUpdate();
            prep.close();
        } finally {
            connection.commit();
        }
    }

    @Override
    public int sweep() throws IOException {
        try {
            return sweepFromDatabase();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private int sweepFromDatabase() throws SQLException {
        try {
            int count = 0;
            PreparedStatement prep = connection.prepareStatement("select id from datastore_meta where lastMod < ?");
            prep.setLong(1, minLastModified);
            ResultSet rs = prep.executeQuery();
            ArrayList<String> ids = new ArrayList<String>();
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
            prep = connection.prepareStatement("delete from datastore_meta where id = ?");
            PreparedStatement prepData = connection.prepareStatement("delete from datastore_data where id = ?");
            for (String id : ids) {
                prep.setString(1, id);
                prep.execute();
                prepData.setString(1, id);
                prepData.execute();
                count++;
            }
            prepData.close();
            prep.close();
            minLastModified = 0;
            return count;
        } finally {
            connection.commit();
        }
    }

    @Override
    public boolean deleteChunk(String chunkId, long maxLastModifiedTime) throws Exception {
        try {
            PreparedStatement prep = null;
            PreparedStatement prepData = null;

            if (maxLastModifiedTime > 0) {
                prep = connection.prepareStatement(
                        "delete from datastore_meta where id = ? and lastMod <= ?");
                prep.setLong(2, maxLastModifiedTime);

                prepData = connection.prepareStatement(
                        "delete from datastore_data where id = ? and lastMod <= ?");
                prepData.setLong(2, maxLastModifiedTime);
            } else {
                prep = connection.prepareStatement(
                        "delete from datastore_meta where id = ?");
                prepData = connection.prepareStatement(
                        "delete from datastore_data where id = ?");
            }
            prep.setString(1, chunkId);
            prep.execute();
            prepData.setString(1, chunkId);
            prepData.execute();

            prep.close();
            prepData.close();
        } finally {
            connection.commit();
        }

        return true;
    }

    @Override
    public Iterator<String> getAllChunkIds(long maxLastModifiedTime) throws Exception {
        PreparedStatement prep = null;

        if (maxLastModifiedTime > 0) {
            prep = connection.prepareStatement(
                    "select id from datastore_meta where lastMod <= ?");
            prep.setLong(1, maxLastModifiedTime);
        } else {
            prep = connection.prepareStatement(
                    "select id from datastore_meta");
        }

        final ResultSet rs = prep.executeQuery();

        return new AbstractIterator<String>() {
            protected String computeNext() {
                try {
                    if (rs.next()) {
                        return rs.getString(1);
                    } else {
                        rs.close();
                    }
                } catch (SQLException e) {
                    try {
                        if ((rs != null) && !rs.isClosed()) {
                            rs.close();
                        }
                    } catch (Exception e2) {
                    }
                    throw new RuntimeException(e);
                }
                return endOfData();
            }
        };
    }
}
