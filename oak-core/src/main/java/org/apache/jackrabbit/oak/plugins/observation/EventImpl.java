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

package org.apache.jackrabbit.oak.plugins.observation;

import java.util.Collections;
import java.util.Map;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.observation.JackrabbitEvent;

/**
 * TODO document
 */
class EventImpl implements JackrabbitEvent {

    private final int type;
    private final String jcrPath;
    private final String userID;
    private final String identifier;
    private final Map<?, ?> info;
    private final long date;
    private final String userData;
    private final boolean external;

    EventImpl(
            int type, String jcrPath, String userID, String identifier,
            Map<?, ?> info, long date, String userData, boolean external) {
        this.type = type;
        this.jcrPath = jcrPath;
        this.userID = userID;
        this.identifier = identifier;
        this.info = info == null ? Collections.emptyMap() : info;
        this.date = date;
        this.userData = userData;
        this.external = external;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public String getPath() throws RepositoryException {
        return jcrPath;
    }

    @Override
    public String getUserID() {
        return userID;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public Map<?, ?> getInfo() {
        return info;
    }

    @Override
    public String getUserData() {
        return userData;
    }

    @Override
    public long getDate() {
        return date;
    }

    @Override
    public synchronized boolean isExternal() {
        return external;
    }

    @Override
    public final boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        EventImpl that = (EventImpl) other;
        return date == that.date && type == that.type &&
                (identifier == null ? that.identifier == null : identifier.equals(that.identifier)) &&
                (info == null ? that.info == null : info.equals(that.info)) &&
                (jcrPath == null ? that.jcrPath == null : jcrPath.equals(that.jcrPath)) &&
                (userID == null ? that.userID == null : userID.equals(that.userID)) &&
                (userData == null ? that.userData == null : userData.equals(that.userData)) &&
                external == that.external;

    }

    @Override
    public final int hashCode() {
        int result = type;
        result = 31 * result + (jcrPath == null ? 0 : jcrPath.hashCode());
        result = 31 * result + (userID == null ? 0 : userID.hashCode());
        result = 31 * result + (identifier == null ? 0 : identifier.hashCode());
        result = 31 * result + (info == null ? 0 : info.hashCode());
        result = 31 * result + (int) (date ^ (date >>> 32));
        result = 31 * result + (userData == null ? 0 :  userData.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return "EventImpl{" +
                "type=" + type +
                ", jcrPath='" + jcrPath + '\'' +
                ", userID='" + userID + '\'' +
                ", identifier='" + identifier + '\'' +
                ", info=" + info +
                ", date=" + date +
                ", userData=" + userData +
                ", external=" + external +
                '}';
    }

}
