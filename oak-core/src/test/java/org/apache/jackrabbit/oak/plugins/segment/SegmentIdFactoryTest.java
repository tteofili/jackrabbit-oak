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
package org.apache.jackrabbit.oak.plugins.segment;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentIdFactory.isBulkSegmentId;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentIdFactory.isDataSegmentId;

import java.util.Set;
import java.util.UUID;

import org.junit.Test;

public class SegmentIdFactoryTest {

    private final SegmentIdFactory factory = new SegmentIdFactory();

    @Test
    public void segmentIdType() {
        assertTrue(isDataSegmentId(factory.newDataSegmentId()));
        assertTrue(isBulkSegmentId(factory.newBulkSegmentId()));

        assertFalse(isBulkSegmentId(factory.newDataSegmentId()));
        assertFalse(isDataSegmentId(factory.newBulkSegmentId()));
    }

    @Test
    public void internedSegmentIds() {
        assertTrue(factory.getSegmentId(0, 0) == factory.getSegmentId(0, 0));
        assertTrue(factory.getSegmentId(1, 2) == factory.getSegmentId(1, 2));
        assertTrue(factory.getSegmentId(1, 2) != factory.getSegmentId(3, 4));
    }

    @Test
    public void referencedSegmentIds() throws InterruptedException {
        UUID a = factory.newDataSegmentId();
        UUID b = factory.newBulkSegmentId();
        UUID c = factory.newDataSegmentId();

        Set<UUID> ids = factory.getReferencedSegmentIds();
        assertTrue(ids.contains(a));
        assertTrue(ids.contains(b));
        assertTrue(ids.contains(c));

        // the returned set is a snapshot in time, not continuously updated
        assertFalse(ids.contains(factory.newBulkSegmentId()));
    }

    /**
     * This test can't be enabled in general, as gc() contract is too
     * weak for this to work reliably. But it's a good manual check for
     * the correct operation of the tracking of segment id references.
     */
    // @Test
    public void garbageCollection() {
        UUID a = factory.newDataSegmentId();
        UUID b = factory.newBulkSegmentId();

        // generate lots of garbage copies of an UUID to get the
        // garbage collector to reclaim also the original instance
        for (int i = 0; i < 1000000; i++) {
            a = new UUID(
                    a.getMostSignificantBits(), a.getLeastSignificantBits());
        }
        System.gc();

        // now the original UUID should no longer be present
        Set<UUID> ids = factory.getReferencedSegmentIds();
        assertFalse(ids.contains(a));
        assertTrue(ids.contains(b));
    }

}
