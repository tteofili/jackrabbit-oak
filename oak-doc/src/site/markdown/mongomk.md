<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
  -->

Oak MongoMK
===========

This module contains a `MicroKernel` implementation using MongoDB to persist
content.

Content Model
-------------

The `MongoMK` stores each node in a separate MongoDB document and updates to
a node are stored by adding new revision/value pairs to the document. This way
the previous state of a node is preserved and can still be retrieved by a
session looking at a given snapshot (revision) of the repository.

The basic MongoDB document of a node in Oak looks like this:

    {
        "_id" : "1:/node",
        "_deleted" : {
            "r13f3875b5d1-0-1" : "false"
        },
        "_lastRev" : {
            "r0-0-1" : "r13f3875b5d1-0-1"
        },
        "_modified" : NumberLong(274208361),
        "_modCount" : NumberLong(1),
        "_revisions" : {
            "r13f3875b5d1-0-1" : "c"
        }
    }

All fields in the above document are metadata and are not exposed through the
Oak API. MongoMK has two types of fields. Simple fields are key/value pairs
like the `_id` or `_modified` field. Versioned fields are kept in sub-documents
where the key is a revision paired with the value at this revision.

The `_id` field is used as the primary key and consists of a combination of the
depth of the path and the path itself. This is an optimization to align sibling
keys in the index.

The `_deleted` sub-document contains the revision this node was created in. In
the above example the root node was created in revision `r13f3875b5d1-0-1`. If
the node is later deleted, the `_deleted` sub-document will get a new field with
the revision the node was deleted in.

The sub-document `_lastRev` contains the last revision written to this node by
each cluster node. In the above example the MongoMK cluster node with id `1`
modified the node the last time in revision `r13f3875b5d1-0-1`, when it created
the node. The revision key in the `_lastRev` sub-document is synthetic and the
only information actually used by MongoMK is the clusterId. The `_lastRev`
sub-document is only updated for non-branch commits or on merge, when changes
become visible to all readers.

The `_modified` field contains an indexed low-resolution timestamp when the node was last modified. The time resolution is five seconds. This field is also updated when
a branch commit modifies a node.

The `_modCount` field contains a modification counter, which is incremented with every change to the document. This field allows MongoMK to perform conditional updates without requesting the whole document.

Finally, the `_revisions` sub-document contains commit information about changes
marked with a revision. E.g. the single entry in the above document tells us
that everything marked with revision `r13f3875b5d1-0-1` is committed and
therefore valid. In case the change is done in a branch then the value would be the
base revision. It is only added for those nodes which happen to be the commit root
for any given commit.

Adding a property `prop` with value `foo` to the node in a next step will
result in the following document:

    {
        "_deleted" : {
            "r13f3875b5d1-0-1" : "false"
        },
        "_id" : "1:/node",
        "_lastRev" : {
            "r0-0-1" : "r13f38818ab6-0-1"
        },
        "_modified" : NumberLong(274208516),
        "_modCount" : NumberLong(2),
        "_revisions" : {
            "r13f3875b5d1-0-1" : "c",
            "r13f38818ab6-0-1" : "c"
        },
        "prop" : {
            "r13f38818ab6-0-1" : "\"foo\""
        }
    }

Now the document contains a new sub-document with the name of the new property.
The value of the property is annotated with the revision the property was set.
With each successful commit to this node, a new field is added to the
`_revisions` sub-document. Similarly the `_lastRev` sub-document and `_modified`
field are updated.

After the node is deleted the document looks like this:

    {
        "_deleted" : {
            "r13f3875b5d1-0-1" : "false",
            "r13f38835063-2-1" : "true"
        },
        "_id" : "1:/node",
        "_lastRev" : {
            "r0-0-1" : "r13f38835063-2-1"
        },
        "_modified" : NumberLong(274208539),
        "_modCount" : NumberLong(3),
        "_revisions" : {
            "r13f3875b5d1-0-1" : "c",
            "r13f38818ab6-0-1" : "c",
            "r13f38835063-2-1" : "c"
        },
        "prop" : {
            "r13f38818ab6-0-1" : "\"foo\""
        }
    }

The `_deleted` sub-document now contains a `r13f38835063-2-1` field marking the
node as deleted in this revision.

Reading the node in previous revisions is still possible, even if it is now
marked as deleted as of revision `r13f38835063-2-1`.

Revisions
---------

As seen in the examples above, a revision is a String and may look like this:
`r13f38835063-2-1`. It consists of three parts:

* A timestamp derived from the system time of the machine it was generated on: `13f38835063`
* A counter to distinguish revisions created with the same timestamp: `-2`
* The cluster node id where this revision was created: `-1`

Branches
--------

MicroKernel implementations support branches, which allows a client to stage
multiple commits and make them visible with a single merge call. In MongoMK
a branch commit looks very similar to a regular commit, but instead of setting
the value of an entry in `_revisions` to `c` (committed), it marks it with
the base revision of the branch commit. In contrast to regular commits where
the commit root is the common ancestor of all nodes modified in a commit, the
commit root of a branch commit is always the root node. This is because a
branch will likely have multiple commits and a commit root must already be
known when the first commit happens on a branch. To make sure the following
branch commits can use the same commit root, MongoMK simply picks the root
node, which always works in this case.

A root node may look like this:

    {
        "_deleted" : {
            "r13fcda88ac0-0-1" : "false",
        },
        "_id" : "0:/",
        "_lastRev" : {
            "r0-0-1" : "r13fcda91720-0-1"
        },
        "_modified" : NumberLong(274708995),
        "_modCount" : NumberLong(2),
        "_revisions" : {
            "r13fcda88ac0-0-1" : "c",
            "r13fcda91720-0-1" : "c"
        },
        "prop" : {
            "r13fcda91720-0-1" : "\"foo\""
        }
    }

The root node was created in revision `r13fcda88ac0-0-1` and later
in revision `r13fcda91720-0-1` property `prop` was set to `foo`.
To keep the example simple, we now assume a branch is created based
on the revision the root node was last modified and a branch commit
is done to modify the existing property. After the branch commit
the root node looks like this:

    {
        "_deleted" : {
            "r13fcda88ac0-0-1" : "false",
        },
        "_id" : "0:/",
        "_lastRev" : {
            "r0-0-1" : "r13fcda91720-0-1"
        },
        "_modified" : NumberLong(274708995),
        "_modCount" : NumberLong(3),
        "_revisions" : {
            "r13fcda88ac0-0-1" : "c",
            "r13fcda91720-0-1" : "c",
			"r13fcda919eb-0-1" : "r13fcda91720-0-1"
        },
        "prop" : {
            "r13fcda91720-0-1" : "\"foo\"",
			"r13fcda919eb-0-1" : "\"bar\"",
        }
    }

At this point the modified property is only visible to a reader
when it reads with the branch revision `r13fcda919eb-0-1` because
the revision is marked with the base version of this commit in
the `_revisions` sub-document. Note, the `_lastRev` is not updated
for branch commits but only when a branch is merged.

When the branch is later merged, the root node will look like this:

    {
        "_deleted" : {
            "r13fcda88ac0-0-1" : "false",
        },
        "_id" : "0:/",
        "_lastRev" : {
            "r0-0-1" : "r13fcda91b12-0-1"
        },
        "_modified" : NumberLong(274708995),
        "_modCount" : NumberLong(4),
        "_revisions" : {
            "r13fcda88ac0-0-1" : "c",
            "r13fcda91720-0-1" : "c",
			"r13fcda919eb-0-1" : "c-r13fcda91b12-0-1"
        },
        "prop" : {
            "r13fcda91720-0-1" : "\"foo\"",
			"r13fcda919eb-0-1" : "\"bar\"",
        }
    }

Now, the changed property is visible to readers with a revision equal or
newer than `r13fcda91b12-0-1`.

The same logic is used for changes to other nodes that belong to a branch
commit. MongoMK internally resolves the commit revision for a modification
before it decides whether a reader is able to see a given change.

Previous Documents
------------------

Over time the size of a document grows because MongoMK adds data to the document
with every modification, but never deletes anything to keep the history. Old data
is moved when there are 1000 commits to be moved or the document is bigger than
1 MB. A document with a reference to old data looks like this:

    {
        "_deleted" : {
            "r13fcda88ac0-0-1" : "false",
        },
        "_id" : "0:/",
        "_lastRev" : {
            "r0-0-1" : "r13fcda91b12-0-1"
        },
        "_modified" : NumberLong(274708995),
        "_modCount" : NumberLong(1004),
        "_revisions" : {
            "r13fcda88ac0-0-1" : "c",
            "r13fcda91720-0-1" : "c",
			"r13fcda919eb-0-1" : "c-r13fcda91b12-0-1"
        },
        "_prev" : {
            "r13fcda88ae0-0-1" : "r13fcda91710-0-1"
        },
        "prop" : {
            "r13fcda91720-0-1" : "\"foo\"",
			"r13fcda919eb-0-1" : "\"bar\"",
        }
    }

The optional sub-document `_prev` contains a list of revision pairs, each
indicating the range of commit revisions a previous document contains. In
the above example there is one document with previous commits from
`r13fcda88ae0-0-1` to `r13fcda91710-0-1`. The id of the previous document
is derived from the upper bound of the range and the id/path of the current
document. The id of the previous document for `r13fcda88ae0-0-1` and `0:/`
is `1:p/r13fcda88ae0-0-1` and may looks like this:

    {
        "_id" : "1:p/r13fcda88ae0-0-1",
        "_modCount" : NumberLong(1),
        "_revisions" : {
            "r13fcda88ae0-0-1" : "c",
            "r13fcda88af0-0-1" : "c",
            ...  
			"r13fcda91710-0-1" : "c"
        },
        "prop" : {
            "r13fcda88ae0-0-1" : "\"foo\"",
            "r13fcda88af0-0-1" : "\"bar\"",
            ...
			"r13fcda91710-0-1" : "\"baz\""
        }
    }

Previous documents only contain immutable data, which means it only contains
committed and merged `_revisions`. This also means the previous ranges of
committed data may overlap because branch commits are not moved to previous
documents until the branch is merged.
 

Background Operations
---------------------
Each MongoMK instance connecting to same database in Mongo server performs certain background task.

### Renew Cluster Id Lease

### Background Document Split

MongoMK periodically checks documents for their size and if necessary splits them up and
moves old data to a previous document. This is done in the background by each MongoMK
instance for the data it created.

### Background Writes

While performing commits there are certain nodes which are modified but do not become part
of commit. For example when a node under /a/b/c is updated then the `_lastRev` property
of all ancestors also need to be updated to the commit revision. Such changes are accumulated
and flushed periodically through a asynchronous job.

### Background Reads

MongoMK periodically picks up changes from other MongoMK instances by polling the root node
for changes of `_lastRev`. This happens once every second.

Pending Topics
--------------

### Conflict Detection and Handling


License
-------

(see the top-level [LICENSE.txt](../LICENSE.txt) for full license details)

Collective work: Copyright 2013 The Apache Software Foundation.

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
