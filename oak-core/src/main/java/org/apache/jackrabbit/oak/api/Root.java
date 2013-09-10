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
package org.apache.jackrabbit.oak.api;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.commit.CommitHook;

/**
 * A {@code Root} instance serves as a container for a {@link Tree}. It is
 * obtained from a {@link ContentSession}, which governs accessibility and
 * visibility of the {@code Tree} and its sub trees.
 * <p>
 * All root instances created by a content session become invalid after the
 * content session is closed. Any method called on an invalid root instance
 * will throw an {@code InvalidStateException}.
 * <p>
 * {@link Tree} instances may become non existing after a call to
 * {@link #refresh()}, {@link #rebase()} or {@link #commit(CommitHook... hooks)}.
 * Any write access to non existing {@code Tree} instances will cause an
 * {@code InvalidStateException}.
 * @see Tree Existence and iterability of trees
 */
public interface Root {

    /**
     * Move the child located at {@code sourcePath} to a child at {@code destPath}.
     * Both paths must be absolute and resolve to a child located beneath this
     * root.<br>
     *
     * This method does nothing and returns {@code false} if
     * <ul>
     *     <li>the tree at {@code sourcePath} does not exist or is not accessible,</li>
     *     <li>the parent of the tree at {@code destinationPath} does not exist or is not accessible,</li>
     *     <li>a tree already exists at {@code destinationPath}.</li>
     * </ul>
     * If a tree at {@code destinationPath} exists but is not accessible to the
     * editing content session this method succeeds but a subsequent
     * {@link #commit(CommitHook... hooks)} will detect the violation and fail.
     *
     * @param sourcePath The source path
     * @param destPath The destination path
     * @return {@code true} on success, {@code false} otherwise.
     */
    boolean move(String sourcePath, String destPath);

    /**
     * Copy the child located at {@code sourcePath} to a child at {@code destPath}.
     * Both paths must be absolute and resolve to a child located in this root.<br>
     *
     * This method does nothing an returns {@code false} if
     * <ul>
     *     <li>The tree at {@code sourcePath} does exist or is not accessible,</li>
     *     <li>the parent of the tree at {@code destinationPath} does not exist or is not accessible,</li>
     *     <li>a tree already exists at {@code destinationPath}.</li>
     * </ul>
     * If a tree at {@code destinationPath} exists but is not accessible to the
     * editing content session this method succeeds but a subsequent
     * {@link #commit(CommitHook... hooks)} will detect the violation and fail.
     *
     * @param sourcePath source path
     * @param destPath destination path
     * @return  {@code true} on success, {@code false} otherwise.
     */
    boolean copy(String sourcePath, String destPath);

    /**
     * Retrieve the possible non existing {@code Tree} at the given absolute {@code path}.
     * The path must resolve to a tree in this root.
     *
     * @param path absolute path to the tree
     * @return tree at the given path.
     */
    @Nonnull
    Tree getTree(@Nonnull String path);

    /**
     * Rebase this root instance to the latest revision. After a call to this method,
     * trees obtained through {@link #getTree(String)} may become non existing.
     */
    void rebase();

    /**
     * Reverts all changes made to this root and refreshed to the latest trunk.
     * After a call to this method, trees obtained through {@link #getTree(String)}
     * may become non existing.
     */
    void refresh();

    /**
     * Atomically persists all changes made to the tree contained in this root to the underlying
     * store.
     * <p>
     * Before any changes are actually persisted the passed commit hooks are run and may fail the
     * commit by throwing a {@code CommitFailedException}. The commit hooks are run in the order as
     * passed and <em>before</em> any other commit hook that might be present in this root.
     * <p>
     * After a successful operation the root is automatically {@link #refresh() refreshed}, such
     * that trees obtained through {@link #getTree(String)} may become non existing.
     *
     * @param hooks  commit hooks to run before any changes are persisted.
     * @throws CommitFailedException
     */
    void commit(CommitHook... hooks) throws CommitFailedException;

    /**
     * Determine whether there are changes on this tree
     * @return  {@code true} iff this tree was modified
     */
    boolean hasPendingChanges();

    /**
     * Get the query engine.
     * 
     * @return the query engine
     */
    @Nonnull
    QueryEngine getQueryEngine();

    /**
     * Returns the blob factory (TODO: review if that really belongs to the OAK-API. see also todos on BlobFactory)
     *
     * @return the blob factory.
     */
    @Nonnull
    BlobFactory getBlobFactory();
    
    /**
     * Get the {@code ContentSession} from which this root was acquired
     * 
     * @return the associated ContentSession
     * 
     * @throws UnsupportedOperationException
     */
    @Nonnull
    ContentSession getContentSession();
}
