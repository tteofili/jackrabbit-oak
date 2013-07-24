/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr.delegate;

import java.io.IOException;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.ItemExistsException;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.IdentifierManager;
import org.apache.jackrabbit.oak.jcr.security.AccessManager;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * TODO document
 */
public class SessionDelegate {
    static final Logger log = LoggerFactory.getLogger(SessionDelegate.class);

    private final ContentSession contentSession;
    private final long refreshInterval;
    private final Root root;
    private final IdentifierManager idManager;
    private final Exception initStackTrace;

    private boolean isAlive = true;
    private int sessionOpCount;
    private long updateCount = 0;

    private long lastAccessed = System.currentTimeMillis();
    private boolean warnIfIdle = true;
    private boolean refreshAtNextAccess = false;

    /**
     * Create a new session delegate for a {@code ContentSession}. The refresh behaviour of the
     * session is governed by the value of the {@code refreshInterval} argument: if the session
     * has been idle longer than that value, an implicit refresh will take place.
     * @param contentSession  the content session
     * @param refreshInterval  refresh interval in seconds or {@code -1} for never.
     */
    public SessionDelegate(@Nonnull ContentSession contentSession, long refreshInterval) {
        this.contentSession = checkNotNull(contentSession);
        this.refreshInterval = refreshInterval;
        this.root = contentSession.getLatestRoot();
        this.idManager = new IdentifierManager(root);
        this.initStackTrace = new Exception("The session was created here:");
    }

    public synchronized void refreshAtNextAccess() {
        refreshAtNextAccess = true;
    }

    /**
     * Performs the passed {@code SessionOperation} in a safe execution context. This
     * context ensures that the session is refreshed if necessary and that refreshing
     * occurs before the session operation is performed and the refreshing is done only
     * once.
     *
     * @param sessionOperation  the {@code SessionOperation} to perform
     * @param <T>  return type of {@code sessionOperation}
     * @return  the result of {@code sessionOperation.perform()}
     * @throws RepositoryException
     */
    public synchronized <T> T perform(SessionOperation<T> sessionOperation)
            throws RepositoryException {
        // Synchronize to avoid conflicting refreshes from concurrent JCR API calls
        if (sessionOpCount == 0) {
            // Refresh and checks only for non re-entrant session operations
            long now = System.currentTimeMillis();
            long timeElapsed = now - lastAccessed;
            // Don't refresh if this operation is a refresh operation itself
            if (!sessionOperation.isRefresh()) {
                if (warnIfIdle && !refreshAtNextAccess
                        && timeElapsed > MILLISECONDS.convert(1, MINUTES)) {
                    // Warn once if this session has been idle too long
                    log.warn("This session has been idle for " + MINUTES.convert(timeElapsed, MILLISECONDS) +
                            " minutes and might be out of date. Consider using a fresh session or explicitly" +
                            " refresh the session.", initStackTrace);
                    warnIfIdle = false;
                }
                if (refreshAtNextAccess || timeElapsed >= refreshInterval) {
                    // Refresh if forced or if the session has been idle too long
                    refreshAtNextAccess = false;
                    refresh(true);
                    updateCount++;
                }
            }
            lastAccessed = now;
            sessionOperation.checkPreconditions();
        }
        try {
            sessionOpCount++;
            return sessionOperation.perform();
        } finally {
            sessionOpCount--;
            if (sessionOperation.isUpdate()) {
                updateCount++;
            }
        }
    }

    @Nonnull
    public ContentSession getContentSession() {
        return contentSession;
    }

    /**
     * Determine whether this session is alive and has not been logged
     * out or become stale by other means.
     * @return {@code true} if this session is alive, {@code false} otherwise.
     */
    public boolean isAlive() {
        return isAlive;
    }

    /**
     * Check that this session is alive.
     * @throws RepositoryException if this session is not alive
     * @see #isAlive()
     */
    public void checkAlive() throws RepositoryException {
        if (!isAlive()) {
            throw new RepositoryException("This session has been closed.");
        }
    }

    /**
     * @return session update counter
     */
    public long getUpdateCount() {
        return updateCount;
    }

    public void checkProtectedNode(String path) throws RepositoryException {
        NodeDelegate node = getNode(path);
        if (node == null) {
            throw new PathNotFoundException(
                    "Node " + path + " does not exist.");
        } else if (node.isProtected()) {
            throw new ConstraintViolationException(
                    "Node " + path + " is protected.");
        }
    }

    @Nonnull
    public AuthInfo getAuthInfo() {
        return contentSession.getAuthInfo();
    }

    public void logout() {
        if (!isAlive) {
            // ignore
            return;
        }

        isAlive = false;
        // TODO

        try {
            contentSession.close();
        } catch (IOException e) {
            log.warn("Error while closing connection", e);
        }
    }

    @Nonnull
    public IdentifierManager getIdManager() {
        return idManager;
    }

    @CheckForNull
    public NodeDelegate getRootNode() {
        return getNode("/");
    }

    /**
     * {@code NodeDelegate} at the given path
     * @param path Oak path
     * @return  The {@code NodeDelegate} at {@code path} or {@code null} if
     * none exists or not accessible.
     */
    @CheckForNull
    public NodeDelegate getNode(String path) {
        return NodeDelegate.create(this, root.getTree(path));
    }

    /**
     * Returns the node or property delegate at the given path.
     *
     * @param path Oak path
     * @return node or property delegate, or {@code null} if none exists
     */
    @CheckForNull
    public ItemDelegate getItem(String path) {
        String name = PathUtils.getName(path);
        if (name.isEmpty()) {
            return getRootNode();
        } else {
            Tree parent = root.getTree(PathUtils.getParentPath(path));
            if (parent.hasProperty(name)) {
                return PropertyDelegate.create(this, parent, name);
            }
            Tree child = parent.getChild(name);
            if (child.exists()) {
                return NodeDelegate.create(this, child);
            } else {
                return null;
            }
        }
    }

    @CheckForNull
    public NodeDelegate getNodeByIdentifier(String id) {
        Tree tree = idManager.getTree(id);
        return (tree == null || !tree.exists()) ? null : new NodeDelegate(this, tree);
    }

    /**
     * {@code PropertyDelegate} at the given path
     * @param path Oak path
     * @return  The {@code PropertyDelegate} at {@code path} or {@code null} if
     * none exists or not accessible.
     */
    @CheckForNull
    public PropertyDelegate getProperty(String path) {
        Tree parent = root.getTree(PathUtils.getParentPath(path));
        String name = PathUtils.getName(path);
        return PropertyDelegate.create(this, parent, name);
    }

    public boolean hasPendingChanges() {
        return root.hasPendingChanges();
    }

    public void save() throws RepositoryException {
        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw newRepositoryException(e);
        }
    }

    public void refresh(boolean keepChanges) {
        if (keepChanges) {
            root.rebase();
        } else {
            root.refresh();
        }
    }

    //----------------------------------------------------------< Workspace >---

    @Nonnull
    public String getWorkspaceName() {
        return contentSession.getWorkspaceName();
    }

    /**
     * Copy a node
     * @param srcPath  oak path to the source node to copy
     * @param destPath  oak path to the destination
     * @throws RepositoryException
     */
    public void copy(String srcPath, String destPath, AccessManager accessManager) throws RepositoryException {
        // check destination
        Tree dest = root.getTree(destPath);
        if (dest.exists()) {
            throw new ItemExistsException(destPath);
        }

        // check parent of destination
        String destParentPath = PathUtils.getParentPath(destPath);
        Tree destParent = root.getTree(destParentPath);
        if (!destParent.exists()) {
            throw new PathNotFoundException(PathUtils.getParentPath(destPath));
        }

        // check source exists
        Tree src = root.getTree(srcPath);
        if (!src.exists()) {
            throw new PathNotFoundException(srcPath);
        }

        accessManager.checkPermissions(destPath, Permissions.getString(Permissions.NODE_TYPE_MANAGEMENT));

        try {
            Root currentRoot = contentSession.getLatestRoot();
            if (!currentRoot.copy(srcPath, destPath)) {
                throw new RepositoryException("Cannot copy node at " + srcPath + " to " + destPath);
            }
            currentRoot.commit();
            refresh(false);
        } catch (CommitFailedException e) {
            throw newRepositoryException(e);
        }
    }

    /**
     * Move a node
     * @param srcPath  oak path to the source node to copy
     * @param destPath  oak path to the destination
     * @param transientOp  whether or not to perform the move in transient space
     * @throws RepositoryException
     */
    public void move(String srcPath, String destPath, boolean transientOp, AccessManager accessManager)
            throws RepositoryException {

        Root moveRoot = transientOp ? root : contentSession.getLatestRoot();

        // check destination
        Tree dest = moveRoot.getTree(destPath);
        if (dest.exists()) {
            throw new ItemExistsException(destPath);
        }

        // check parent of destination
        String destParentPath = PathUtils.getParentPath(destPath);
        Tree destParent = moveRoot.getTree(destParentPath);
        if (!destParent.exists()) {
            throw new PathNotFoundException(PathUtils.getParentPath(destPath));
        }

        // check source exists
        Tree src = moveRoot.getTree(srcPath);
        if (!src.exists()) {
            throw new PathNotFoundException(srcPath);
        }

        accessManager.checkPermissions(destPath, Permissions.getString(Permissions.NODE_TYPE_MANAGEMENT));

        try {
            if (!moveRoot.move(srcPath, destPath)) {
                throw new RepositoryException("Cannot move node at " + srcPath + " to " + destPath);
            }
            if (!transientOp) {
                moveRoot.commit();
                refresh(true);
            }
        } catch (CommitFailedException e) {
            throw newRepositoryException(e);
        }
    }

    @Nonnull
    public QueryEngine getQueryEngine() {
        return root.getQueryEngine();
    }

    //-----------------------------------------------------------< internal >---

    @Nonnull  // FIXME this should be package private. OAK-672
    public Root getRoot() {
        return root;
    }

    /**
     * Wraps the given {@link CommitFailedException} instance using the
     * appropriate {@link RepositoryException} subclass based on the
     * {@link CommitFailedException#getType() type} of the given exception.
     *
     * @param exception typed commit failure exception
     * @return matching repository exception
     */
    private static RepositoryException newRepositoryException(CommitFailedException exception) {
        return exception.asRepositoryException();
    }

}
