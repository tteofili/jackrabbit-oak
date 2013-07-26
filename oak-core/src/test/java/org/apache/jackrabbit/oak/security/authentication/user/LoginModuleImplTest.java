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
package org.apache.jackrabbit.oak.security.authentication.user;

import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * LoginTest...
 */
public class LoginModuleImplTest extends AbstractSecurityTest {

    @Override
    protected Configuration getConfiguration() {
        return ConfigurationUtil.getDefaultConfiguration(ConfigurationParameters.EMPTY);
    }

    @Test
    public void testNullLogin() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(null);
            fail("Null login should fail");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testGuestLogin() throws Exception {
        ContentSession cs = login(new GuestCredentials());
        try {
            AuthInfo authInfo = cs.getAuthInfo();
            String anonymousID = UserUtil.getAnonymousId(getUserConfiguration().getParameters());
            assertEquals(anonymousID, authInfo.getUserID());
        } finally {
            cs.close();
        }
    }

    @Test
    public void testAnonymousLogin() throws Exception {
        String anonymousID = UserUtil.getAnonymousId(getUserConfiguration().getParameters());

        UserManager userMgr = getUserManager(root);

        // verify initial user-content looks like expected
        Authorizable anonymous = userMgr.getAuthorizable(anonymousID);
        assertNotNull(anonymous);
        assertFalse(root.getTree(anonymous.getPath()).hasProperty(UserConstants.REP_PASSWORD));

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(anonymousID, new char[0]));
            fail("Login with anonymousID should fail since the initial setup doesn't provide a password.");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testUserLogin() throws Exception {
        UserManager userManager = getUserManager(root);
        ContentSession cs = null;
        User user = null;
        try {
            user = userManager.createUser("test", "pw");
            root.commit();

            cs = login(new SimpleCredentials("test", "pw".toCharArray()));
            AuthInfo authInfo = cs.getAuthInfo();
            assertEquals("test", authInfo.getUserID());
        } finally {
            if (user != null) {
                user.remove();
                root.commit();
            }
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testSelfImpersonation() throws Exception {
        UserManager userManager = getUserManager(root);
        ContentSession cs = null;
        User user = null;
        try {
            user = userManager.createUser("test", "pw");
            root.commit();

            SimpleCredentials sc = new SimpleCredentials("test", "pw".toCharArray());
            cs = login(sc);

            AuthInfo authInfo = cs.getAuthInfo();
            assertEquals("test", authInfo.getUserID());

            cs.close();

            sc = new SimpleCredentials("test", new char[0]);
            ImpersonationCredentials ic = new ImpersonationCredentials(sc, authInfo);
            cs = login(ic);

            authInfo = cs.getAuthInfo();
            assertEquals("test", authInfo.getUserID());
        } finally {
            if (user != null) {
                user.remove();
                root.commit();
            }
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testInvalidImpersonation() throws Exception {
        UserManager userManager = getUserManager(root);
        ContentSession cs = null;
        User user = null;
        try {
            user = userManager.createUser("test", "pw");
            root.commit();

            SimpleCredentials sc = new SimpleCredentials("test", "pw".toCharArray());
            cs = login(sc);

            AuthInfo authInfo = cs.getAuthInfo();
            assertEquals("test", authInfo.getUserID());

            cs.close();
            cs = null;

            ConfigurationParameters config = securityProvider.getConfiguration(UserConfiguration.class).getParameters();
            String adminId = UserUtil.getAdminId(config);
            sc = new SimpleCredentials(adminId, new char[0]);
            ImpersonationCredentials ic = new ImpersonationCredentials(sc, authInfo);

            try {
                cs = login(ic);
                fail("User 'test' should not be allowed to impersonate " + adminId);
            } catch (LoginException e) {
                // success
            }
        } finally {
            if (user != null) {
                user.remove();
                root.commit();
            }
            if (cs != null) {
                cs.close();
            }
        }
    }
}
