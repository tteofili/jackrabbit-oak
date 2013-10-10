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
package org.apache.jackrabbit.oak.plugins.mongomk.util;

import org.apache.jackrabbit.oak.plugins.mongomk.Revision;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Utils}.
 */
public class UtilsTest {

    @Test
    public void getPreviousIdFor() {
        Revision r = new Revision(System.currentTimeMillis(), 0, 0);
        assertEquals("1:p/" + r.toString(), Utils.getPreviousIdFor("0:/", r));
        assertEquals("2:p/test/" + r.toString(), Utils.getPreviousIdFor("1:/test", r));
    }
}
