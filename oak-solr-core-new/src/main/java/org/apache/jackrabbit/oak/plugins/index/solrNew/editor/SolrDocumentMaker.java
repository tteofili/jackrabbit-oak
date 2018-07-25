/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.solrNew.editor;

import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.Aggregate.NodeIncludeResult;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.FulltextBinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextDocumentMaker;
import org.apache.jackrabbit.oak.plugins.index.search.util.DataConversionUtil;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrDocumentMaker extends FulltextDocumentMaker<SolrInputDocument> {

    private static final Logger LOG = LoggerFactory.getLogger(SolrDocumentMaker.class);

    public SolrDocumentMaker(FulltextBinaryTextExtractor textExtractor, IndexDefinition definition,
            IndexingRule indexingRule, String path) {
        super(textExtractor, definition, indexingRule, path);
    }

    @Override
    protected SolrInputDocument initDoc() {
        return new SolrInputDocument();
    }

    @Override
    protected SolrInputDocument finalizeDoc(SolrInputDocument doc, boolean dirty, boolean facet) {
        return doc;
    }

    @Override
    protected boolean isFacetingEnabled() {
        // TODO don't know how to do facets
        return false;
    }

    @Override
    protected boolean indexTypeOrderedFields(SolrInputDocument doc, String pname, int tag, PropertyState property, PropertyDefinition pd) {
        if (!includePropertyValue(property, 0, pd)) {
            return false;
        }
        String name = FieldNames.createDocValFieldName(pname);
        try {
            if (tag == Type.LONG.tag()) {
                //TODO Distinguish fields which need to be used for search and for sort
                //If a field is only used for Sort then it can be stored with less precision
                doc.addField(name, property.getValue(Type.LONG));
                return true;
            } else if (tag == Type.DATE.tag()) {
                String date = property.getValue(Type.DATE);
                doc.addField(name, DataConversionUtil.dateToLong(date));
                return true;
            } else if (tag == Type.DOUBLE.tag()) {
                doc.addField(name, property.getValue(Type.DOUBLE));
                return true;
            } else if (tag == Type.BOOLEAN.tag()) {
                doc.addField(name, property.getValue(Type.BOOLEAN).toString());
                return true;
            } else if (tag == Type.STRING.tag()) {
                doc.addField(name, property.getValue(Type.STRING));
                return true;
            }
        } catch (Exception e) {
            LOG.warn(
                    "[{}] Ignoring ordered property {} of type {} for path {} as multivalued ordered property not supported",
                    getIndexName(), pname,
                    Type.fromTag(property.getType().tag(), true), path);
        }
        return false;
    }

    @Override
    protected boolean addBinary(SolrInputDocument doc, String path, List<String> binaryValues) {
        boolean added = false;
        for (String binaryValue : binaryValues) {
            if (path != null) {
                doc.addField(FieldNames.createFulltextFieldName(path), binaryValue);
            } else {
                doc.addField(FieldNames.FULLTEXT, binaryValue);
            }
            added = true;
        }
        return added;
    }

    @Override
    protected boolean indexFacetProperty(SolrInputDocument doc, int tag, PropertyState property, String pname) {
        // TODO don't know how to do facets
        return false;
    }

    @Override
    protected void indexAnalyzedProperty(SolrInputDocument doc, String pname, String value, PropertyDefinition pd) {
         // TODO this duplicates the oak-lucene method constructAnalyzedPropertyName(String pname)
        String fieldName;
        if (definition.getVersion().isAtLeast(IndexFormatVersion.V2)) {
            fieldName = FieldNames.createAnalyzedFieldName(pname);
        } else {
            fieldName = pname;
        }
        // TODO need to consider skipTokenization, stored
        // fields.add(newPropertyField(analyzedPropName, value, !pd.skipTokenization(pname), pd.stored));
        doc.setField(fieldName, value);
    }

    @Override
    protected void indexSuggestValue(SolrInputDocument doc, String value) {
        doc.addField(FieldNames.SUGGEST, value);
    }

    @Override
    protected void indexSpellcheckValue(SolrInputDocument doc, String value) {
        // TODO fields.add(newPropertyField(FieldNames.SPELLCHECK, value, true, false));
        doc.addField(FieldNames.SPELLCHECK, value);
    }

    @Override
    protected void indexFulltextValue(SolrInputDocument doc, String value) {
        // TODO Field field = newFulltextField(value); - not stored
        doc.addField(FieldNames.FULLTEXT, value);
    }

    @Override
    protected boolean indexTypedProperty(SolrInputDocument doc, PropertyState property, String pname,
            PropertyDefinition pd) {
        if (!includePropertyValue(property, 0, pd)) {
            return false;
        }
        int tag = property.getType().tag();
        // TODO use Field.Store.NO
        if (tag == Type.LONG.tag()) {
            doc.addField(pname, property.getValue(Type.LONG));
            return true;
        } else if (tag == Type.DATE.tag()) {
            String date = property.getValue(Type.DATE);
            doc.addField(pname, DataConversionUtil.dateToLong(date));
            return true;
        } else if (tag == Type.DOUBLE.tag()) {
            doc.addField(pname, property.getValue(Type.DOUBLE));
            return true;
        } else if (tag == Type.BOOLEAN.tag()) {
            doc.addField(pname, property.getValue(Type.BOOLEAN).toString());
            return true;
        } else if (tag == Type.STRING.tag()) {
            doc.addField(pname, property.getValue(Type.STRING));
            return true;
        }
        return false;
    }

    @Override
    protected void indexNotNullProperty(SolrInputDocument doc, PropertyDefinition pd) {
        // TODO fields.add(new StringField(FieldNames.NOT_NULL_PROPS, pd.name, Field.Store.NO));
        doc.addField(FieldNames.NOT_NULL_PROPS, pd.name);
    }

    @Override
    protected void indexNullProperty(SolrInputDocument doc, PropertyDefinition pd) {
        // TODO fields.add(new StringField(FieldNames.NULL_PROPS, pd.name, Field.Store.NO));
        doc.addField(FieldNames.NULL_PROPS, pd.name);
    }

    @Override
    protected void indexAggregateValue(SolrInputDocument doc, NodeIncludeResult result, String value,
            PropertyDefinition pd) {
//        Field field = result.isRelativeNode() ?
//                newFulltextField(result.rootIncludePath, value) : newFulltextField(value) ;
//        if (pd != null) {
//            field.setBoost(pd.boost);
//        }
//        fields.add(field);
        String fieldName = result.isRelativeNode() ? result.rootIncludePath : FieldNames.FULLTEXT;
        doc.addField(fieldName, value, pd.boost);
    }

    @Override
    protected void indexNodeName(SolrInputDocument doc, String value) {
        // TODO fields.add(new StringField(FieldNames.NODE_NAME, value, Field.Store.NO));
        doc.addField(FieldNames.NODE_NAME, value);
    }

    @Override
    protected void indexAncestors(SolrInputDocument doc, String path) {
        // TODO don't store
        doc.addField(FieldNames.ANCESTORS, PathUtils.getParentPath(path));
        doc.addField(FieldNames.PATH_DEPTH, PathUtils.getDepth(path));
    }

    @Override
    protected boolean augmentCustomFields(String path, SolrInputDocument doc, NodeState document) {
        // TODO not supported currently
        return false;
    }

}
