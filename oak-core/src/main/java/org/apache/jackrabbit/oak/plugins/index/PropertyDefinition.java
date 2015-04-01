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
package org.apache.jackrabbit.oak.plugins.index;

/**
 * The definition of the handling of a certain property within a certain {@link org.apache.jackrabbit.oak.spi.query.QueryIndex}
 */
public interface PropertyDefinition {

    /**
     * returns the property name
     *
     * @return the name of the property.
     */
    String getName();

    /**
     * returns whether the name of this property definition (as defined in #getName)
     * is or not a regular expression
     *
     * @return {@code true} if this definition's name is reg.ex., {@code false} otherwies
     */
    boolean isRegexp();

    /**
     * returns whether the property identified by this definition is ordered or not
     *
     * @return {@code true} if this definition's property is ordered, {@code false} otherwies
     */
    boolean isOrdered();

    /**
     * Returns the property type. If no explicit type is defined the default is assumed
     * to be {@link javax.jcr.PropertyType#STRING}
     *
     * @return propertyType as per javax.jcr.PropertyType
     */
    int getType();
}
