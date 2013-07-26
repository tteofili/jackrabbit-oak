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
package org.apache.jackrabbit.oak.spi.security.user.util;

import org.junit.Test;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PasswordUtilTest {

    private static List<String> PLAIN_PWDS = new ArrayList<String>();
    static {
        PLAIN_PWDS.add("pw");
        PLAIN_PWDS.add("PassWord123");
        PLAIN_PWDS.add("_");
        PLAIN_PWDS.add("{invalidAlgo}");
        PLAIN_PWDS.add("{invalidAlgo}Password");
        PLAIN_PWDS.add("{SHA-256}");
        PLAIN_PWDS.add("pw{SHA-256}");
        PLAIN_PWDS.add("p{SHA-256}w");
        PLAIN_PWDS.add("");
    }

    private static Map<String, String> HASHED_PWDS = new HashMap<String, String>();
    static {
        for (String pw : PLAIN_PWDS) {
            try {
                HASHED_PWDS.put(pw, PasswordUtil.buildPasswordHash(pw));
            } catch (Exception e) {
                // should not get here
            }
        }
    }

    @Test
    public void testBuildPasswordHash() throws Exception {
        for (String pw : PLAIN_PWDS) {
            String pwHash = PasswordUtil.buildPasswordHash(pw);
            assertFalse(pw.equals(pwHash));
        }

        List<Integer[]> l = new ArrayList<Integer[]>();
        l.add(new Integer[] {0, 1000});
        l.add(new Integer[] {1, 10});
        l.add(new Integer[] {8, 50});
        l.add(new Integer[] {10, 5});
        l.add(new Integer[] {-1, -1});
        for (Integer[] params : l) {
            for (String pw : PLAIN_PWDS) {
                int saltsize = params[0];
                int iterations = params[1];

                String pwHash = PasswordUtil.buildPasswordHash(pw, PasswordUtil.DEFAULT_ALGORITHM, saltsize, iterations);
                assertFalse(pw.equals(pwHash));
            }
        }
    }

    @Test
    public void testBuildPasswordHashInvalidAlgorithm() throws Exception {
        List<String> invalidAlgorithms = new ArrayList<String>();
        invalidAlgorithms.add("");
        invalidAlgorithms.add("+");
        invalidAlgorithms.add("invalid");

        for (String invalid : invalidAlgorithms) {
            try {
                String pwHash = PasswordUtil.buildPasswordHash("pw", invalid, PasswordUtil.DEFAULT_SALT_SIZE, PasswordUtil.DEFAULT_ITERATIONS);
                fail("Invalid algorithm " + invalid);
            } catch (NoSuchAlgorithmException e) {
                // success
            }
        }

    }

    @Test
    public void testIsPlainTextPassword() throws Exception {
        for (String pw : PLAIN_PWDS) {
            assertTrue(pw + " should be plain text.", PasswordUtil.isPlainTextPassword(pw));
        }
    }

    @Test
    public void testIsPlainTextForNull() throws Exception {
        assertTrue(PasswordUtil.isPlainTextPassword(null));
    }

    @Test
    public void testIsPlainTextForPwHash() throws Exception {
        for (String pwHash : HASHED_PWDS.values()) {
            assertFalse(pwHash + " should not be plain text.", PasswordUtil.isPlainTextPassword(pwHash));
        }
    }

    @Test
    public void testIsSame() throws Exception {
        for (String pw : HASHED_PWDS.keySet()) {
            String pwHash = HASHED_PWDS.get(pw);
            assertTrue("Not the same " + pw + ", " + pwHash, PasswordUtil.isSame(pwHash, pw));
        }

        String pw = "password";
        String pwHash = PasswordUtil.buildPasswordHash(pw, "SHA-1", 4, 50);
        assertTrue("Not the same '" + pw + "', " + pwHash, PasswordUtil.isSame(pwHash, pw));

        pwHash = PasswordUtil.buildPasswordHash(pw, "md5", 0, 5);
        assertTrue("Not the same '" + pw + "', " + pwHash, PasswordUtil.isSame(pwHash, pw));

        pwHash = PasswordUtil.buildPasswordHash(pw, "md5", -1, -1);
        assertTrue("Not the same '" + pw + "', " + pwHash, PasswordUtil.isSame(pwHash, pw));
    }

    @Test
    public void testIsNotSame() throws Exception {
        String previous = null;
        for (String pw : HASHED_PWDS.keySet()) {
            String pwHash = HASHED_PWDS.get(pw);
            assertFalse(pw, PasswordUtil.isSame(pw, pw));
            assertFalse(pwHash, PasswordUtil.isSame(pwHash, pwHash));
            if (previous != null) {
                assertFalse(previous, PasswordUtil.isSame(pwHash, previous));
            }
            previous = pw;
        }
    }
    
    @Test
    public void testPBKDF2WithHmacSHA1() throws Exception {
        String algo = "PBKDF2WithHmacSHA1";
        // test vector from http://tools.ietf.org/html/rfc6070
        String hash = PasswordUtil.generateHash(
                "pass\0word", algo,
                PasswordUtil.convertBytesToHex("sa\0lt".getBytes()), 4096);
        assertEquals("{PBKDF2WithHmacSHA1}7361006c74-4096-56fa6aa75548099dcc37d7f03425e0c3", hash);
    }
    
}