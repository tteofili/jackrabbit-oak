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
package org.apache.jackrabbit.oak.jcr.operation;

import javax.jcr.RepositoryException;

/**
 * A {@code SessionOperation} provides an execution context for executing session scoped operations.
 */
public abstract class SessionOperation<T> {

    private final boolean update;

    protected SessionOperation(boolean update) {
        this.update = update;
    }

    protected SessionOperation() {
        this(false);
    }

    /**
     * Returns {@code true} if this operation updates the the transient
     * @return
     */
    public boolean isUpdate() {
        return update;
    }

    /**
     * Return {@code true} if this operation refreshed the transient space
     * @return
     */
    public boolean isRefresh() {
        return false;
    }

    public void checkPreconditions() throws RepositoryException {
    }

    public abstract T perform() throws RepositoryException;

}
