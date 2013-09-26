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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A revision.
 */
public class Revision {

    private static volatile long lastTimestamp;

    private static volatile long lastRevisionTimestamp;
    private static volatile int lastRevisionCount;
    
    /**
     * The timestamp in milliseconds since 1970 (unlike in seconds as in
     * MongoDB). The timestamp is local to the machine that generated the
     * revision, such that timestamps of revisions can only be compared if the
     * machine id is the same.
     */
    private long timestamp;
    
    /**
     * An incrementing counter, for commits that occur within the same
     * millisecond.
     */
    private int counter;
    
    /**
     * The cluster id (the MongoDB machine id).
     */
    private int clusterId;

    /**
     * Whether this is a branch revision.
     */
    private final boolean branch;
    
    public Revision(long timestamp, int counter, int clusterId) {
        this(timestamp, counter, clusterId, false);
    }
    
    public Revision(long timestamp, int counter, int clusterId, boolean branch) {
        this.timestamp = timestamp;
        this.counter = counter;
        this.clusterId = clusterId;
        this.branch = branch;
    }

    /**
     * Compare the time part of two revisions. If they contain the same time,
     * the counter is compared.
     * 
     * @return -1 if this revision occurred earlier, 1 if later, 0 if equal
     */
    int compareRevisionTime(Revision other) {
        int comp = timestamp < other.timestamp ? -1 : timestamp > other.timestamp ? 1 : 0;
        if (comp == 0) {
            comp = counter < other.counter ? -1 : counter > other.counter ? 1 : 0;
        }
        return comp;
    }
    
    /**
     * Create a simple revision id. The format is similar to MongoDB ObjectId.
     * 
     * @param clusterId the unique machineId + processId
     * @return the unique revision id
     */
    static Revision newRevision(int clusterId) {
        long timestamp = getCurrentTimestamp();
        int c;
        synchronized (Revision.class) {
            if (timestamp == lastRevisionTimestamp) {
                c = ++lastRevisionCount;
            } else {
                lastRevisionTimestamp = timestamp;
                lastRevisionCount = c = 0;
            }
        }
        return new Revision(timestamp, c, clusterId);
    }
    
    /**
     * Get the timestamp value of the current date and time. Within the same
     * process, the returned value is never smaller than a previously returned
     * value, even if the system time was changed.
     * 
     * @return the timestamp
     */
    public static long getCurrentTimestamp() {
        long timestamp = System.currentTimeMillis();
        if (timestamp < lastTimestamp) {
            // protect against decreases in the system time,
            // time machines, and other fluctuations in the time continuum
            timestamp = lastTimestamp;
        } else if (timestamp > lastTimestamp) {
            lastTimestamp = timestamp;
        }
        return timestamp;
    }
    
    /**
     * Get the difference between two timestamps (a - b) in milliseconds.
     * 
     * @param a the first timestamp
     * @param b the second timestamp
     * @return the difference in milliseconds
     */
    public static long getTimestampDifference(long a, long b) {
        return a - b;
    }
    
    public static Revision fromString(String rev) {
        boolean isBranch = false;
        if (rev.startsWith("b")) {
            isBranch = true;
            rev = rev.substring(1);
        }
        if (!rev.startsWith("r")) {
            throw new IllegalArgumentException(rev);
        }
        int idxCount = rev.indexOf('-');
        if (idxCount < 0) {
            throw new IllegalArgumentException(rev);
        }
        int idxClusterId = rev.indexOf('-', idxCount + 1);
        if (idxClusterId < 0) {
            throw new IllegalArgumentException(rev);
        }
        String t = rev.substring(1, idxCount);
        long timestamp = Long.parseLong(t, 16);
        t = rev.substring(idxCount + 1, idxClusterId);
        int c = Integer.parseInt(t, 16);
        t = rev.substring(idxClusterId + 1);
        int clusterId = Integer.parseInt(t, 16);
        Revision r = new Revision(timestamp, c, clusterId, isBranch);
        return r;
    }
    
    @Override
    public String toString() {
        return (branch ? "b" : "") + 'r' + Long.toHexString(timestamp) + '-' +
                Integer.toHexString(counter) + '-' + Integer.toHexString(clusterId);
    }
    
    /**
     * Get the timestamp in milliseconds since 1970.
     * 
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    public int getCounter() {
        return counter;
    }

    /**
     * @return <code>true</code> if this is a branch revision, otherwise
     *         <code>false</code>.
     */
    public boolean isBranch() {
        return branch;
    }

    /**
     * Returns a revision with the same timestamp, counter and clusterId as this
     * revision and the branch flag set to <code>true</code>.
     *
     * @return branch revision with this timestamp, counter and clusterId.
     */
    public Revision asBranchRevision() {
        if (isBranch()) {
            return this;
        } else {
            return new Revision(timestamp, counter, clusterId, true);
        }
    }

    /**
     * Returns a revision with the same timestamp, counter and clusterId as this
     * revision and the branch flag set to <code>false</code>.
     *
     * @return trunkrevision with this timestamp, counter and clusterId.
     */
    public Revision asTrunkRevision() {
        if (!isBranch()) {
            return this;
        } else {
            return new Revision(timestamp, counter, clusterId);
        }
    }

    @Override
    public int hashCode() {
        return (int) (timestamp >>> 32) ^ (int) timestamp ^ counter ^ clusterId;
    }
    
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other == null) {
            return false;
        } else if (other.getClass() != this.getClass()) {
            return false;
        }
        Revision r = (Revision) other;
        return r.timestamp == this.timestamp && 
                r.counter == this.counter && 
                r.clusterId == this.clusterId &&
                r.branch == this.branch;
    }

    public boolean equalsIgnoreBranch(Revision other) {
        if (this == other) {
            return true;
        } else if (other == null) {
            return false;
        }
        return other.timestamp == this.timestamp &&
                other.counter == this.counter &&
                other.clusterId == this.clusterId;
    }

    public int getClusterId() {
        return clusterId;
    }
    
    /**
     * Revision ranges allow to compare revisions ids of different cluster instances. A
     * range tells when a list of revisions from a certain cluster instance was seen by
     * the current process.
     */
    static class RevisionRange {
        
        /**
         * The newest revision for the given cluster instance and time.
         */
        Revision revision;

        /**
         * The (local) revision; the time when this revision was seen by this
         * cluster instance.
         */
        Revision seenAt;
        
        @Override
        public String toString() {
            return revision + ":" + seenAt;
        }
        
    }
    
    /**
     * A facility that is able to compare revisions of different cluster instances.
     * It contains a map of revision ranges.
     */
    public static class RevisionComparator implements Comparator<Revision> {

        private static final Revision NEWEST = new Revision(Long.MAX_VALUE, 0, 0);

        private static final Revision FUTURE = new Revision(Long.MAX_VALUE, Integer.MAX_VALUE, 0);
        
        /**
         * The map of cluster instances to lists of revision ranges.
         */
        private final ConcurrentMap<Integer, List<RevisionRange>> map = 
                new ConcurrentHashMap<Integer, List<RevisionRange>>();
        
        /**
         * When comparing revisions that occurred before, the timestamp is ignored.
         */
        private long oldestTimestamp;
        
        /**
         * The cluster node id of the current cluster node. Revisions 
         * from this cluster node that are newer than the newest range
         * (new local revisions) 
         * are considered to be the newest revisions overall.
         */
        private final int currentClusterNodeId;

        RevisionComparator(int currentClusterNodId) {
            this.currentClusterNodeId = currentClusterNodId;
        }
        
        /**
         * Forget the order of older revisions. After calling this method, when comparing
         * revisions that happened before the given value, the timestamp order is used
         * (time dilation is ignored for older events).
         * 
         * @param timestamp the time in milliseconds (see {@link #getCurrentTimestamp})
         */
        public void purge(long timestamp) {
            oldestTimestamp = timestamp;
            for (int clusterId : map.keySet()) {
                while (true) {
                    List<RevisionRange> list = map.get(clusterId);
                    List<RevisionRange> newList = purge(list);
                    if (newList == null) {
                        // retry if removing was not successful
                        if (map.remove(clusterId, list)) {
                            break;
                        }
                    } else if (newList == list) {
                        // no change
                        break;
                    } else {
                        // retry if replacing was not successful
                        if (map.replace(clusterId, list, newList)) {
                            break;
                        }
                    }
                }
            } 
        }

        private List<RevisionRange> purge(List<RevisionRange> list) {
            int i = 0;
            for (; i < list.size(); i++) {
                RevisionRange r = list.get(i);
                if (r.seenAt.getTimestamp() > oldestTimestamp) {
                    break;
                }
            }
            if (i > list.size() - 1) {
                return null;
            } else if (i == 0) {
                return list;
            }
            return new ArrayList<RevisionRange>(list.subList(i, list.size()));
        }
        
        /**
         * Add the revision to the top of the queue for the given cluster node.
         * If an entry for this timestamp already exists, it is replaced.
         * 
         * @param r the revision
         * @param seenAt the (local) revision where this revision was seen here
         */
        public void add(Revision r, Revision seenAt) {
            int clusterId = r.getClusterId();
            while (true) {
                List<RevisionRange> list = map.get(clusterId);
                List<RevisionRange> newList;
                if (list == null) {
                    newList = new ArrayList<RevisionRange>();
                } else {
                    RevisionRange last = list.get(list.size() - 1);
                    if (last.seenAt.equals(seenAt)) {
                        // replace existing
                        if (r.compareRevisionTime(last.revision) > 0) {
                            // but only if newer
                            last.revision = r;
                        }
                        return;
                    }
                    if (last.revision.compareRevisionTime(r) > 0) {
                        throw new IllegalArgumentException("Can not add an earlier revision");
                    }
                    newList = new ArrayList<RevisionRange>(list);
                }
                RevisionRange range = new RevisionRange();
                range.seenAt = seenAt;
                range.revision = r;
                newList.add(range);
                if (list == null) {
                    if (map.putIfAbsent(clusterId, newList) == null) {
                        return;
                    }                    
                } else {
                    if (map.replace(clusterId, list, newList)) {
                        return;
                    }
                }
            }
        }
        
        @Override
        public int compare(Revision o1, Revision o2) {
            if (o1.getClusterId() == o2.getClusterId()) {
                return o1.compareRevisionTime(o2);
            }
            Revision range1 = getRevisionSeen(o1);
            Revision range2 = getRevisionSeen(o2);
            if (range1 == null || range2 == null) {
                return o1.compareRevisionTime(o2);
            }
            int comp = range1.compareRevisionTime(range2);
            if (comp != 0) {
                return comp;
            }
            return Integer.signum(o1.getClusterId() - o2.getClusterId());
        }
        
        /**
         * Get the timestamp from the revision range, if found. If no range was
         * found for this cluster instance, or if the revision is older than the
         * earliest range, then 0 is returned. If the revision is newer than the
         * newest range for this cluster instance, then Long.MAX_VALUE is
         * returned.
         * 
         * @param r the revision
         * @return the revision where it was seen, null if not found, 
         *      the timestamp plus 1 second for new local revisions;
         *      Long.MAX_VALUE for new non-local revisions (meaning 'in the future')
         */
        private Revision getRevisionSeen(Revision r) {
            List<RevisionRange> list = map.get(r.getClusterId());
            if (list == null) {
                return null;
            }
            // search from latest backward
            // (binary search could be used, but we expect most queries
            // at the end of the list)
            Revision result = null;
            for (int i = list.size() - 1; i >= 0; i--) {
                RevisionRange range = list.get(i);
                int compare = r.compareRevisionTime(range.revision);
                if (compare > 0) {
                    if (i == list.size() - 1) {
                        // newer than the newest range
                        if (r.getClusterId() == currentClusterNodeId) {
                            // newer than all others, except for FUTURE
                            return NEWEST;
                        }
                        // happenes in the future (not visible yet)
                        return FUTURE;
                    }
                    break;
                }
                result = range.seenAt;
            }
            return result;
        }
        
        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder();
            for (int clusterId : new TreeSet<Integer>(map.keySet())) {
                int i = 0;
                buff.append(clusterId).append(":");
                for (RevisionRange r : map.get(clusterId)) {
                    if (i++ % 4 == 0) {
                        buff.append('\n');
                    }
                    buff.append(" ").append(r);
                }
                buff.append("\n");
            }
            return buff.toString();
        }
        
    }

}
