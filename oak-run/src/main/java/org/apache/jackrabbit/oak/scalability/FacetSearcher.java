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
package org.apache.jackrabbit.oak.scalability;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;

/**
 * Scalability test for facet query implementation
 */
public class FacetSearcher extends SearchScalabilityBenchmark {

    @Override
    protected Query getQuery(@Nonnull QueryManager qm, ScalabilityAbstractSuite.ExecutionContext context) throws RepositoryException {

        final String statement = "select [jcr:path], [facet(jcr:primaryType)] from [nt:base]";

        LOG.debug("statement: {}", statement);

        return qm.createQuery(statement, Query.JCR_SQL2);
    }


}
