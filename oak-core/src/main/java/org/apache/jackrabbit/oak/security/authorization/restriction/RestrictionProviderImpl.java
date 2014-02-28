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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.AbstractRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.CompositePattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinitionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;

/**
 * Default restriction provider implementation that supports the following
 * restrictions:
 *
 * <ul>
 *     <li>{@link #REP_GLOB}: A simple paths matching pattern. See {@link GlobPattern}
 *     for details.</li>
 *     <li>{@link #REP_NT_NAMES}: A restriction that allows to limit the effect
 *     of a given access control entries to JCR nodes of any of the specified
 *     primary node type. In case of a JCR property the primary type of the
 *     parent node is taken into consideration when evaluating the permissions.</li>
 * </ul>
 */
@Component
@Service(RestrictionProvider.class)
public class RestrictionProviderImpl extends AbstractRestrictionProvider {

    public RestrictionProviderImpl() {
        super(supportedRestrictions());
    }

    private static Map<String, RestrictionDefinition> supportedRestrictions() {
        RestrictionDefinition glob = new RestrictionDefinitionImpl(REP_GLOB, Type.STRING, false);
        RestrictionDefinition nts = new RestrictionDefinitionImpl(REP_NT_NAMES, Type.NAMES, false);
        RestrictionDefinition pfxs = new RestrictionDefinitionImpl(REP_PREFIXES, Type.STRINGS, false);
        return ImmutableMap.of(glob.getName(), glob, nts.getName(), nts, pfxs.getName(), pfxs);
    }

    //------------------------------------------------< RestrictionProvider >---

    @Override
    public RestrictionPattern getPattern(String oakPath, Tree tree) {
        if (oakPath == null) {
            return RestrictionPattern.EMPTY;
        } else {
            PropertyState glob = tree.getProperty(REP_GLOB);

            List<RestrictionPattern> patterns = new ArrayList<RestrictionPattern>(2);
            if (glob != null) {
                patterns.add(GlobPattern.create(oakPath, glob.getValue(Type.STRING)));
            }
            PropertyState ntNames = tree.getProperty(REP_NT_NAMES);
            if (ntNames != null) {
                patterns.add(new NodeTypePattern(ntNames.getValue(Type.NAMES)));
            }

            PropertyState prefixes = tree.getProperty(REP_PREFIXES);
            if (prefixes != null) {
                patterns.add(new PrefixPattern(prefixes.getValue(Type.STRINGS)));
            }

            switch (patterns.size()) {
                case 1 : return patterns.get(0);
                case 2 : return new CompositePattern(patterns);
                default : return  RestrictionPattern.EMPTY;
            }
        }
    }
}
