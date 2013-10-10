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
package org.apache.jackrabbit.oak.query.ast;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;

/**
 * The function "localname(..)".
 */
public class NodeLocalNameImpl extends DynamicOperandImpl {

    private final String selectorName;
    private SelectorImpl selector;

    public NodeLocalNameImpl(String selectorName) {
        this.selectorName = selectorName;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "localname(" + quote(selectorName) + ')';
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
    }
    
    @Override
    public PropertyExistenceImpl getPropertyExistence() {
        return null;
    }
    
    @Override
    public Set<SelectorImpl> getSelectors() {
        return Collections.singleton(selector);
    }

    @Override
    public PropertyValue currentProperty() {
        String name = PathUtils.getName(selector.currentPath());
        int colon = name.indexOf(':');
        // TODO LOCALNAME: evaluation of local name might not be correct
        String localName = colon < 0 ? name : name.substring(colon + 1);
        return PropertyValues.newString(localName);
    }

    @Override
    public void restrict(FilterImpl f, Operator operator, PropertyValue v) {
        // TODO support LOCALNAME index conditions
    }
    
    @Override
    public void restrictList(FilterImpl f, List<PropertyValue> list) {
        // optimizations of type "LOCALNAME(..) IN(A, B)" are not supported
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl s) {
        return s == selector;
    }
    
    @Override
    int getPropertyType() {
        return PropertyType.STRING;
    }

    @Override
    public DynamicOperandImpl createCopy() {
        return new NodeLocalNameImpl(selectorName);
    }

}
