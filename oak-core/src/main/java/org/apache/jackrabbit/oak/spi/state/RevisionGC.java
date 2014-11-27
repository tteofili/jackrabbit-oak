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

package org.apache.jackrabbit.oak.spi.state;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.management.ManagementOperation.done;
import static org.apache.jackrabbit.oak.management.ManagementOperation.newManagementOperation;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.oak.management.ManagementOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link RevisionGCMBean} based on a {@code Runnable}.
 */
public class RevisionGC implements RevisionGCMBean {
    private static final Logger log = LoggerFactory.getLogger(RevisionGC.class);

    public static final String OP_NAME = "Revision garbage collection";

    private final Runnable gc;
    private final Executor executor;

    private ManagementOperation<String> gcOp = done(OP_NAME, "");

    /**
     * @param gc               Revision garbage collector
     * @param executor         executor for running the garbage collection task
     */
    public RevisionGC(
            @Nonnull Runnable gc,
            @Nonnull Executor executor) {
        this.gc = checkNotNull(gc);
        this.executor = checkNotNull(executor);
    }

    @Nonnull
    @Override
    public CompositeData startRevisionGC() {
        if (gcOp.isDone()) {
            gcOp = newManagementOperation(OP_NAME, new Callable<String>() {
                @Override
                public String call() throws Exception {
                    gc.run();
                    return "Revision GC initiated";
                }
            });
            executor.execute(gcOp);
        }
        return getRevisionGCStatus();
    }

    @Nonnull
    @Override
    public CompositeData getRevisionGCStatus() {
        return gcOp.getStatus().toCompositeData();
    }
}
