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
package org.apache.jackrabbit.oak.jcr.security.user;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.ItemExistsException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.security.auth.Subject;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;

/**
 * Testing user/group import with default {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior}
 */
public class UserImportTest extends AbstractImportTest {

    @Override
    protected List<String> getPathsToRemove() {
        return ImmutableList.of(
                USERPATH + "/t",
                USERPATH + "/r",
                USERPATH + "/uFolder",
                USERPATH + "/some",
                USERPATH + "/g",
                USERPATH + "/t_diff",
                GROUPPATH + "/g",
                GROUPPATH + "/s",
                GROUPPATH + "/gFolder");
    }

    @Override
    protected String getImportBehavior() {
        return null;
    }

    @Test
    public void testImportUser() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:disabled\" sv:type=\"String\"><sv:value>disabledUser</sv:value></sv:property>" +
                "</sv:node>";

        Node target = adminSession.getNode(USERPATH);
        doImport(USERPATH, xml);

        assertTrue(target.isModified());
        assertTrue(adminSession.hasPendingChanges());

        Authorizable newUser = userMgr.getAuthorizable("t");
        assertNotNull(newUser);
        assertFalse(newUser.isGroup());
        assertEquals("t", newUser.getPrincipal().getName());
        assertEquals("t", newUser.getID());
        assertTrue(((User) newUser).isDisabled());
        assertEquals("disabledUser", ((User) newUser).getDisabledReason());

        Node n = adminSession.getNode(newUser.getPath());
        assertTrue(n.isNew());
        assertTrue(n.getParent().isSame(target));

        assertEquals("t", n.getName());
        assertEquals("t", n.getProperty(UserConstants.REP_PRINCIPAL_NAME).getString());
        assertEquals("{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375", n.getProperty(UserConstants.REP_PASSWORD).getString());
        assertEquals("disabledUser", n.getProperty(UserConstants.REP_DISABLED).getString());

        // saving changes of the import -> must succeed. add mandatory
        // props should have been created.
        adminSession.save();
    }

    @Test
    public void testImportGroup() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>";

        Node target = adminSession.getNode(GROUPPATH);
        doImport(GROUPPATH, xml);

        assertTrue(target.isModified());
        assertTrue(adminSession.hasPendingChanges());

        Authorizable newGroup = userMgr.getAuthorizable("g");
        assertNotNull(newGroup);
        assertTrue(newGroup.isGroup());
        assertEquals("g", newGroup.getPrincipal().getName());
        assertEquals("g", newGroup.getID());

        Node n = adminSession.getNode(newGroup.getPath());
        assertTrue(n.isNew());
        assertTrue(n.getParent().isSame(target));

        assertEquals("g", n.getName());
        assertEquals("g", n.getProperty(UserConstants.REP_PRINCIPAL_NAME).getString());

        // saving changes of the import -> must succeed. add mandatory
        // props should have been created.
        adminSession.save();
    }

    /**
     * @since OAK 1.0 : constraintviolation is no longer detected during import
     *        but only upon save.
     */
    @Test
    public void testImportGroupIntoUsersTree() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>";

        /*
         importing a group below the users-path:
         - nonProtected node rep:Group must be created.
         - protected properties are ignored
         - UserManager.getAuthorizable must return null.
         - saving changes must fail with ConstraintViolationEx.
         */

        Node target = adminSession.getNode(USERPATH);
        doImport(USERPATH, xml);

        assertTrue(target.isModified());
        assertTrue(adminSession.hasPendingChanges());

        Authorizable newGroup = userMgr.getAuthorizable("g");
        assertNotNull(newGroup);
        assertTrue(target.hasNode("g"));
        assertTrue(target.hasProperty("g/rep:principalName"));

        // saving changes of the import -> must fail
        try {
            adminSession.save();
            fail("Import must be incomplete. Saving changes must fail.");
        } catch (ConstraintViolationException e) {
            // success
        }
    }

    @Test
    public void testImportAuthorizableId() throws Exception {
        // importing an authorizable with an jcr:uuid that doesn't match the
        // hash of the given ID -> getAuthorizable(String id) will not find the
        // authorizable.
        //String calculatedUUID = "e358efa4-89f5-3062-b10d-d7316b65649e";
        String mismatchUUID = "a358efa4-89f5-3062-b10d-d7316b65649e";

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>" + mismatchUUID + "</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property></sv:node>";

        Node target = adminSession.getNode(USERPATH);
        doImport(USERPATH, xml);

        assertTrue(target.isModified());
        assertTrue(adminSession.hasPendingChanges());

        // node must be present:
        assertTrue(target.hasNode("t"));
        Node n = target.getNode("t");
        assertEquals(mismatchUUID, n.getUUID());

        // but UserManager.getAuthorizable(String) will not find the
        // authorizable
        Authorizable newUser = userMgr.getAuthorizable("t");
        assertNull(newUser);
    }

    @Test
    public void testExistingPrincipal() throws Exception {
        Principal existing = null;
        PrincipalIterator principalIterator = ((JackrabbitSession) adminSession).getPrincipalManager().getPrincipals(PrincipalManager.SEARCH_TYPE_ALL);
        while (principalIterator.hasNext()) {
            Principal p = principalIterator.nextPrincipal();
            if (userMgr.getAuthorizable(p) != null) {
                existing = p;
                break;
            }
        }
        if (existing == null) {
            throw new NotExecutableException();
        }

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>" + existing.getName() + "</sv:value></sv:property>" +
                "</sv:node>";

        try {
            doImport(USERPATH, xml);
            adminSession.save();

            fail("Import must detect conflicting principals.");
        } catch (RepositoryException e) {
            // success
        }
    }

    @Test
    public void testConflictingPrincipalsWithinImport() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                "   <sv:value>rep:AuthorizableFolder</sv:value>" +
                "</sv:property>" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>" +
                "<sv:node sv:name=\"g1\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0120a4f9-196a-3f9e-b9f5-23f31f914da7</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>" +
                "</sv:node>";

        try {
            doImport(GROUPPATH, xml);
            adminSession.save();

            fail("Import must detect conflicting principals.");
        } catch (RepositoryException e) {
            // success
        }
    }

    @Test
    public void testMultiValuedPrincipalName() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value><sv:value>g2</sv:value><sv:value>g</sv:value></sv:property></sv:node>";

        /*
         importing a group with a multi-valued rep:principalName property
         - nonProtected node rep:Group must be created.
         - property rep:principalName must be created regularly without being protected
         - saving changes must fail with ConstraintViolationEx. as the protected
           mandatory property rep:principalName is missing
         */
        Node target = adminSession.getNode(GROUPPATH);
        doImport(GROUPPATH, xml);

        assertTrue(target.isModified());
        assertTrue(adminSession.hasPendingChanges());

        Authorizable newGroup = userMgr.getAuthorizable("g");
        assertNotNull(newGroup);

        assertTrue(target.hasNode("g"));
        assertTrue(target.hasProperty("g/rep:principalName"));
        assertFalse(target.getProperty("g/rep:principalName").getDefinition().isProtected());

        // saving changes of the import -> must fail as mandatory prop is missing
        try {
            adminSession.save();
            fail("Import must be incomplete. Saving changes must fail.");
        } catch (ConstraintViolationException e) {
            // success
        }
    }

    @Test
    public void testPlainTextPassword() throws Exception {
        String plainPw = "myPassword";
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>" + plainPw + "</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        Node target = adminSession.getNode(USERPATH);
        doImport(USERPATH, xml);

        assertTrue(target.isModified());
        assertTrue(adminSession.hasPendingChanges());

        Authorizable newUser = userMgr.getAuthorizable("t");
        Node n = adminSession.getNode(newUser.getPath());

        String pwValue = n.getProperty(UserConstants.REP_PASSWORD).getString();
        assertFalse(plainPw.equals(pwValue));
        assertTrue(pwValue.toLowerCase().startsWith("{sha"));
    }

    /**
     * @since OAK 1.0 : password property is not longer mandatory -> multivalued
     *        property will just be ignored (instead of throwing ConstraintViolationException
     *        upon save).
     */
    @Test
    public void testMultiValuedPassword() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";
        /*
         importing a user with a multi-valued rep:password property
         - nonProtected node rep:User must be created.
         - property rep:password must be created regularly without being protected
         */
        Node target = adminSession.getNode(USERPATH);
        doImport(USERPATH, xml);

        assertTrue(target.isModified());
        assertTrue(adminSession.hasPendingChanges());

        Authorizable newUser = userMgr.getAuthorizable("t");
        assertNotNull(newUser);

        assertTrue(target.hasNode("t"));
        assertTrue(target.hasProperty("t/rep:password"));
        assertFalse(target.getProperty("t/rep:password").getDefinition().isProtected());
    }

    @Test
    public void testIncompleteUser() throws Exception {
        List<String> incompleteXml = new ArrayList<String>();
        incompleteXml.add("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "</sv:node>");
        incompleteXml.add("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>");

        for (String xml : incompleteXml) {
            Node target = adminSession.getNode(USERPATH);
            try {
                doImport(USERPATH, xml);
                // saving changes of the import -> must fail as mandatory prop is missing
                try {
                    adminSession.save();
                    fail("Import must be incomplete. Saving changes must fail.");
                } catch (ConstraintViolationException e) {
                    // success
                }
            } finally {
                adminSession.refresh(false);
                if (target.hasNode("t")) {
                    target.getNode("t").remove();
                    adminSession.save();
                }
            }
        }
    }

    /**
     * @since OAK 1.0 : importing User without password must succeed.
     */
    @Test
    public void testUserWithoutPassword() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        doImport(USERPATH, xml);

        Authorizable user = userMgr.getAuthorizable("t");
        assertNotNull(user);
        assertFalse(user.isGroup());
        assertFalse(adminSession.propertyExists(user.getPath() + "/rep:password"));
    }

    @Test
    public void testIncompleteGroup() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "</sv:node>";

        /*
         importing a group without rep:principalName property
         - saving changes must fail with ConstraintViolationEx.
         */
        doImport(GROUPPATH, xml);
        // saving changes of the import -> must fail as mandatory prop is missing
        try {
            adminSession.save();
            fail("Import must be incomplete. Saving changes must fail.");
        } catch (ConstraintViolationException e) {
            // success
        }
    }

    @Test
    public void testImportWithIntermediatePath() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"some\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>d5433be9-68d0-4fba-bf96-efc29f461993</sv:value></sv:property>" +
                "<sv:node sv:name=\"intermediate\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>d87354a4-037e-4756-a8fb-deb2eb7c5149</sv:value></sv:property>" +
                "<sv:node sv:name=\"path\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>24263272-b789-4568-957a-3bcaf99dbab3</sv:value></sv:property>" +
                "<sv:node sv:name=\"t3\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0b8854ad-38f0-36c6-9807-928d28195609</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}4358694eeb098c6708ae914a10562ce722bbbc34</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t3</sv:value></sv:property>" +
                "</sv:node>" +
                "</sv:node>" +
                "</sv:node>" +
                "</sv:node>";

        Node target = adminSession.getNode(USERPATH);
        doImport(USERPATH, xml);

        assertTrue(target.isModified());
        assertTrue(adminSession.hasPendingChanges());

        Authorizable newUser = userMgr.getAuthorizable("t3");
        assertNotNull(newUser);
        assertFalse(newUser.isGroup());
        assertEquals("t3", newUser.getPrincipal().getName());
        assertEquals("t3", newUser.getID());

        Node n = adminSession.getNode(newUser.getPath());
        assertTrue(n.isNew());

        Node parent = n.getParent();
        assertFalse(n.isSame(target));
        assertTrue(parent.isNodeType(UserConstants.NT_REP_AUTHORIZABLE_FOLDER));
        assertFalse(parent.getDefinition().isProtected());

        assertTrue(target.hasNode("some"));
        assertTrue(target.hasNode("some/intermediate/path"));
    }

    @Test
    public void testImportNewMembers() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                "   <sv:value>rep:AuthorizableFolder</sv:value>" +
                "</sv:property>" +
                "<sv:node sv:name=\"g\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>" +
                "<sv:node sv:name=\"g1\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0120a4f9-196a-3f9e-b9f5-23f31f914da7</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "</sv:node>" +
                "</sv:node>";

        doImport(GROUPPATH, xml);

        Group g = (Group) userMgr.getAuthorizable("g");
        assertNotNull(g);
        Group g1 = (Group) userMgr.getAuthorizable("g1");
        assertNotNull(g1);

        Node n = adminSession.getNode(g1.getPath());
        assertTrue(n.hasProperty(UserConstants.REP_MEMBERS) || n.hasNode(UserConstants.NT_REP_MEMBERS));

        // getWeakReferences only works upon save.
        adminSession.save();

        assertTrue(g1.isMember(g));
    }

    @Test
    public void testImportNewMembersReverseOrder() throws Exception {
        // group is imported before the member
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "   <sv:node sv:name=\"g1\">" +
                "       <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "       <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0120a4f9-196a-3f9e-b9f5-23f31f914da7</sv:value></sv:property>" +
                "       <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
                "       <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   </sv:node>" +
                "   <sv:node sv:name=\"g\">" +
                "       <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "       <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "       <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "   </sv:node>" +
                "</sv:node>";

        doImport(GROUPPATH, xml);

        Group g = (Group) userMgr.getAuthorizable("g");
        assertNotNull(g);
        Group g1 = (Group) userMgr.getAuthorizable("g1");
        assertNotNull(g1);

        Node n = adminSession.getNode(g1.getPath());
        assertTrue(n.hasProperty(UserConstants.REP_MEMBERS) || n.hasNode(UserConstants.NT_REP_MEMBERS));

        // getWeakReferences only works upon save.
        adminSession.save();

        assertTrue(g1.isMember(g));
    }

    @Test
    public void testImportMembers() throws Exception {
        Authorizable admin = userMgr.getAuthorizable("admin");
        if (admin == null) {
            throw new NotExecutableException();
        }

        String uuid = adminSession.getNode(admin.getPath()).getUUID();
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "   <sv:node sv:name=\"g1\">" +
                "       <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "       <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0120a4f9-196a-3f9e-b9f5-23f31f914da7</sv:value></sv:property>" +
                "       <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
                "       <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>" + uuid + "</sv:value></sv:property>" +
                "   </sv:node>" +
                "</sv:node>";

        doImport(GROUPPATH, xml);

        Group g1 = (Group) userMgr.getAuthorizable("g1");
        assertNotNull(g1);

        // getWeakReferences only works upon save.
        adminSession.save();

        assertTrue(g1.isMember(admin));

        boolean found = false;
        for (Iterator<Group> it = admin.declaredMemberOf(); it.hasNext() && !found; ) {
            found = "g1".equals(it.next().getID());
        }
        assertTrue(found);
    }

    @Test
    public void testImportImpersonation() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"uFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "<sv:node sv:name=\"t\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:impersonators\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>" +
                "<sv:node sv:name=\"g\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>" +
                "</sv:node>";

        doImport(USERPATH, xml);

        Authorizable newUser = userMgr.getAuthorizable("t");
        assertNotNull(newUser);

        Authorizable u2 = userMgr.getAuthorizable("g");
        assertNotNull(u2);

        Subject subj = new Subject();
        subj.getPrincipals().add(u2.getPrincipal());

        Impersonation imp = ((User) newUser).getImpersonation();
        assertTrue(imp.allows(subj));
    }

    @Test
    public void testImportUuidCollisionRemoveExisting() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"r\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>4b43b0ae-e356-34cd-95b9-10189b3dc231</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        doImport(USERPATH, xml);

        // re-import should succeed if UUID-behavior is set accordingly
        doImport(USERPATH, xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

        // saving changes of the import -> must succeed. add mandatory
        // props should have been created.
        adminSession.save();
    }

    /**
     * Same as {@link #testImportUuidCollisionRemoveExisting} with the single
     * difference that the initial import is saved before being overwritten.
     *
     * @throws Exception
     */
    @Test
    public void testImportUuidCollisionRemoveExisting2() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"r\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>4b43b0ae-e356-34cd-95b9-10189b3dc231</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";
        doImport(USERPATH, xml);
        adminSession.save();

        // re-import should succeed if UUID-behavior is set accordingly
        doImport(USERPATH, xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

        // saving changes of the import -> must succeed. add mandatory
        // props should have been created.
        adminSession.save();
    }

    @Test
    public void testImportUuidCollisionThrow() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        try {
            doImport(USERPATH, xml);
            doImport(USERPATH, xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
            fail("UUID collision must be handled according to the uuid behavior.");

        } catch (ItemExistsException e) {
            // success.
        }
    }

    @Test
    public void testImportGroupMembersFromNodes() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"s\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:sling=\"http://sling.apache.org/jcr/sling/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\">" +
                "  <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "  <sv:property sv:name=\"jcr:created\" sv:type=\"Date\"><sv:value>2010-08-17T18:22:20.086+02:00</sv:value></sv:property>" +
                "  <sv:property sv:name=\"jcr:createdBy\" sv:type=\"String\"><sv:value>admin</sv:value></sv:property>" +
                "  <sv:node sv:name=\"sh\">" +
                "     <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "     <sv:property sv:name=\"jcr:created\" sv:type=\"Date\"><sv:value>2010-08-17T18:22:20.086+02:00</sv:value></sv:property>" +
                "     <sv:property sv:name=\"jcr:createdBy\" sv:type=\"String\"><sv:value>admin</sv:value></sv:property>" +
                "     <sv:node sv:name=\"shrimps\">" +
                "        <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "        <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>08429aec-6f09-30db-8c83-1a2a57fc760c</sv:value></sv:property>" +
                "        <sv:property sv:name=\"jcr:created\" sv:type=\"Date\"><sv:value>2010-08-17T18:22:20.086+02:00</sv:value></sv:property>" +
                "        <sv:property sv:name=\"jcr:createdBy\" sv:type=\"String\"><sv:value>admin</sv:value></sv:property>" +
                "        <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>shrimps</sv:value></sv:property>" +
                "        <sv:node sv:name=\"rep:members\">" +
                "           <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property>" +
                "           <sv:node sv:name=\"adi\">" +
                "              <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property>" +
                "              <sv:node sv:name=\"adi\">" +
                "                 <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property>" +
                "                 <sv:property sv:name=\"adi\" sv:type=\"WeakReference\"><sv:value>c46335eb-267e-3e1c-9e5b-017acb4cd799</sv:value></sv:property>" +
                "                 <sv:property sv:name=\"admin\" sv:type=\"WeakReference\"><sv:value>21232f29-7a57-35a7-8389-4a0e4a801fc3</sv:value></sv:property>" +
                "              </sv:node>" +
                "              <sv:node sv:name=\"angi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"angi\" sv:type=\"WeakReference\"><sv:value>a468b64f-b1df-377c-b325-20d97aaa1ad9</sv:value></sv:property><sv:property sv:name=\"anonymous\" sv:type=\"WeakReference\"><sv:value>294de355-7d9d-30b3-92d8-a1e6aab028cf</sv:value></sv:property><sv:property sv:name=\"cati\" sv:type=\"WeakReference\"><sv:value>f08910b6-41c8-3cb9-a648-1dddd14b132d</sv:value></sv:property></sv:node></sv:node>" +
                "              <sv:node sv:name=\"debbi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:node sv:name=\"debbi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"debbi\" sv:type=\"WeakReference\"><sv:value>d53bedf9-ebb8-3117-a8b8-162d32b4bee2</sv:value></sv:property><sv:property sv:name=\"eddi\" sv:type=\"WeakReference\"><sv:value>1795fa1a-3d20-3a64-996e-eaaeb520a01e</sv:value></sv:property><sv:property sv:name=\"gabi\" sv:type=\"WeakReference\"><sv:value>a0d499c7-5105-3663-8611-a32779a57104</sv:value></sv:property><sv:property sv:name=\"hansi\" sv:type=\"WeakReference\"><sv:value>9ea4d671-8ed1-399a-8401-59487a14d00a</sv:value></sv:property></sv:node><sv:node sv:name=\"hari\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"hari\" sv:type=\"WeakReference\"><sv:value>a9bcf1e4-d7b9-3a22-a297-5c812d938889</sv:value></sv:property><sv:property sv:name=\"lisi\" sv:type=\"WeakReference\"><sv:value>dc3a8f16-70d6-3bea-a9b7-b65048a0ac40</sv:value></sv:property></sv:node><sv:node sv:name=\"luzi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"luzi\" sv:type=\"WeakReference\"><sv:value>9ec299fd-3461-3f1a-9749-92a76f2516eb</sv:value></sv:property><sv:property sv:name=\"pipi\" sv:type=" +
                "\"WeakReference\"><sv:value>16d5d24f-5b09-3199-9bd4-e5f57bf11237</sv:value></sv:property><sv:property sv:name=\"susi\" sv:type=\"WeakReference\"><sv:value>536931d8-0dec-318c-b3db-9612bdd004d4</sv:value></sv:property>" +
                "              </sv:node>" +
                "           </sv:node>" +
                "        </sv:node>" +
                "     </sv:node>" +
                "   </sv:node>" +
                "</sv:node>";

        List<String> createdUsers = new LinkedList<String>();
        Node target = adminSession.getNode(GROUPPATH);
        try {
            String[] users = {"angi", "adi", "hansi", "lisi", "luzi", "susi", "pipi", "hari", "gabi", "eddi",
                    "debbi", "cati", "admin", "anonymous"};

            for (String user : users) {
                if (userMgr.getAuthorizable(user) == null) {
                    userMgr.createUser(user, user);
                    createdUsers.add(user);
                }
            }
            if (!userMgr.isAutoSave()) {
                adminSession.save();
            }

            doImport(GROUPPATH, xml);
            if (!userMgr.isAutoSave()) {
                adminSession.save();
            }

            Authorizable aShrimps = userMgr.getAuthorizable("shrimps");
            assertNotNull(aShrimps);
            assertTrue(aShrimps.isGroup());

            Group gShrimps = (Group) aShrimps;
            for (String user : users) {
                assertTrue(user + " should be member of " + gShrimps, gShrimps.isMember(userMgr.getAuthorizable(user)));
            }


        } finally {
            adminSession.refresh(false);
            for (String user : createdUsers) {
                Authorizable a = userMgr.getAuthorizable(user);
                if (a != null && !a.isGroup()) {
                    a.remove();
                }
            }
            if (!userMgr.isAutoSave()) {
                adminSession.save();
            }
            for (NodeIterator it = target.getNodes(); it.hasNext(); ) {
                it.nextNode().remove();
            }
            if (!userMgr.isAutoSave()) {
                adminSession.save();
            }
        }
    }

    /**
     * @since OAK 1.0 : Importing rep:authorizableId
     */
    @Test
    public void testImportUserWithAuthorizableId() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        doImport(USERPATH, xml);

        Authorizable newUser = userMgr.getAuthorizable("t");
        assertNotNull(newUser);
        assertFalse(newUser.isGroup());
        assertEquals("t", newUser.getID());
        assertTrue(adminSession.propertyExists(newUser.getPath() + "/rep:authorizableId"));
        assertEquals("t", adminSession.getProperty(newUser.getPath() + "/rep:authorizableId").getString());
        adminSession.save();
    }

    /**
     * @since OAK 1.0 : Importing rep:authorizableId
     */
    @Test
    public void testImportGroupWithAuthorizableId() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>";

        doImport(GROUPPATH, xml);

        Authorizable newGroup = userMgr.getAuthorizable("g");
        assertNotNull(newGroup);
        assertTrue(newGroup.isGroup());
        assertEquals("g", newGroup.getID());
        assertTrue(adminSession.propertyExists(newGroup.getPath() + "/rep:authorizableId"));
        assertEquals("g", adminSession.getProperty(newGroup.getPath() + "/rep:authorizableId").getString());

        adminSession.save();
    }

    /**
     * @since OAK 1.0 : Importing rep:authorizableId
     */
    @Test
    public void testImportUserWithIdDifferentFromNodeName() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t_diff\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        doImport(USERPATH, xml);

        Authorizable newUser = userMgr.getAuthorizable("t");

        assertNotNull(newUser);
        assertFalse(newUser.isGroup());
        assertEquals("t", newUser.getID());
        assertTrue(adminSession.propertyExists(newUser.getPath() + "/rep:authorizableId"));
        assertEquals("t", adminSession.getProperty(newUser.getPath() + "/rep:authorizableId").getString());
        adminSession.save();
    }

    /**
     * Same as {@link #testImportUserWithIdDifferentFromNodeName} but with
     * different order of properties.
     *
     * @since OAK 1.0 : Importing rep:authorizableId
     */
    @Test
    public void testImportUserWithIdDifferentFromNodeName2() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t_diff\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";
        doImport(USERPATH, xml);

        Authorizable newUser = userMgr.getAuthorizable("t");

        assertNotNull(newUser);
        assertFalse(newUser.isGroup());
        assertEquals("t", newUser.getID());
        assertTrue(adminSession.propertyExists(newUser.getPath() + "/rep:authorizableId"));
        assertEquals("t", adminSession.getProperty(newUser.getPath() + "/rep:authorizableId").getString());
        adminSession.save();
    }

    /**
     * @since OAK 1.0 : Importing rep:authorizableId
     */
    @Test
    public void testImportUserWithExistingId() throws Exception {
        String existingId = "admin";
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t_diff\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>" + existingId + "</sv:value></sv:property>" +
                "</sv:node>";
        try {
            doImport(USERPATH, xml);
            fail("Reuse of existing ID must be detected.");
        } catch (AuthorizableExistsException e) {
            // success
        }
    }

    /**
     * @since OAK 1.0 : Importing rep:authorizableId
     */
    @Test
    public void testImportUserWithIdCollision() throws Exception {
        String collidingId = "t";
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"uFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "<sv:node sv:name=\"t1\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>" + collidingId + "</sv:value></sv:property>" +
                "</sv:node>" +
                "<sv:node sv:name=\"t2\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0f826a89-cf68-3399-85f4-cf320c1a5842</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>" + collidingId + "</sv:value></sv:property>" +
                "</sv:node>" +
                "</sv:node>";
        try {
            doImport(USERPATH, xml);
            fail("Reuse of existing ID must be detected.");
        } catch (AuthorizableExistsException e) {
            // success
        }
    }
}
