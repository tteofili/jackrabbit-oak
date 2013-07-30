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
package org.apache.jackrabbit.oak.core;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher;
import org.apache.jackrabbit.oak.plugins.observation.ChangeDispatcher.Listener;
import org.apache.jackrabbit.oak.plugins.observation.Observable;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContext;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code MicroKernel}-based implementation of the {@link ContentSession} interface.
 */
class ContentSessionImpl implements ContentSession, Observable {

    private static final Logger log = LoggerFactory.getLogger(ContentSessionImpl.class);

    private final LoginContext loginContext;
    private final SecurityProvider securityProvider;
    private final String workspaceName;
    private final NodeStore store;
    private final CommitHook hook;
    private final ChangeDispatcher changeDispatcher;
    private final QueryIndexProvider indexProvider;

    private volatile boolean live = true;

    public ContentSessionImpl(@Nonnull LoginContext loginContext,
                              @Nonnull SecurityProvider securityProvider,
                              @Nonnull String workspaceName,
                              @Nonnull NodeStore store,
                              @Nonnull CommitHook hook,
                              @Nonnull ChangeDispatcher changeDispatcher,
                              @Nonnull QueryIndexProvider indexProvider) {
        this.loginContext = loginContext;
        this.securityProvider = securityProvider;
        this.workspaceName = workspaceName;
        this.store = store;
        this.hook = hook;
        this.changeDispatcher = changeDispatcher;
        this.indexProvider = indexProvider;
    }

    private void checkLive() {
        checkState(live, "This session has been closed");
    }

    //-----------------------------------------------------< ContentSession >---
    @Nonnull
    @Override
    public AuthInfo getAuthInfo() {
        checkLive();
        Set<AuthInfo> infoSet = loginContext.getSubject().getPublicCredentials(AuthInfo.class);
        if (infoSet.isEmpty()) {
            return AuthInfo.EMPTY;
        } else {
            return infoSet.iterator().next();
        }
    }

    @Override
    public String getWorkspaceName() {
        return workspaceName;
    }

    @Nonnull
    @Override
    public Root getLatestRoot() {
        checkLive();
        return new AbstractRoot(store, hook, changeDispatcher.newHook(ContentSessionImpl.this), workspaceName,
                loginContext.getSubject(), securityProvider, indexProvider) {
            @Override
            protected void checkLive() {
                ContentSessionImpl.this.checkLive();
            }

            @Override
            public ContentSession getContentSession() {
            	return ContentSessionImpl.this;
            }
        };
    }

    @Override
    public Listener newListener() {
        return changeDispatcher.newListener();
    }

    //-----------------------------------------------------------< Closable >---
    @Override
    public synchronized void close() throws IOException {
        try {
            loginContext.logout();
            live = false;
        } catch (LoginException e) {
            log.error("Error during logout.", e);
        }
    }

}
