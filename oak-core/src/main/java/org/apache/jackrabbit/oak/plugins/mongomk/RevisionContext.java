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
package org.apache.jackrabbit.oak.plugins.mongomk;

/**
 * Provides revision related context.
 */
public interface RevisionContext {

    /**
     * @return the branches of the local MongoMK instance, which are not yet
     *         merged.
     */
    public UnmergedBranches getBranches();

    /**
     * @return the pending modifications.
     */
    public UnsavedModifications getPendingModifications();

    /**
     * @return the revision comparator.
     */
    public Revision.RevisionComparator getRevisionComparator();

    /**
     * Ensure the revision visible from now on, possibly by updating the head
     * revision, so that the changes that occurred are visible.
     *
     * @param foreignRevision the revision from another cluster node
     * @param changeRevision the local revision that is sorted after the foreign revision
     */
    public void publishRevision(Revision foreignRevision, Revision changeRevision);

    /**
     * @return the cluster id of the local MongoMK instance.
     */
    public int getClusterId();
}
