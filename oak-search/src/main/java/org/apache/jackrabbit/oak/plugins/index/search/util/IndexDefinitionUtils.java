package org.apache.jackrabbit.oak.plugins.index.search.util;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.EXCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.EXPERIMENTAL_STORAGE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INCLUDE_PROPERTY_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PERSISTENCE_FILE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PERSISTENCE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PERSISTENCE_PATH;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

import java.util.Set;

import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A second utility to build index definitions.
 */
public class IndexDefinitionUtils {

    public static NodeBuilder newFTIndexDefinition(
        @NotNull NodeBuilder index, @NotNull String name, String type,
        @Nullable Set<String> propertyTypes) {
        return newFTIndexDefinition(index, name, type, propertyTypes, null, null, null);
    }

    public static NodeBuilder newFTIndexDefinition(
            @NotNull NodeBuilder index, @NotNull String name, String type,
            @Nullable Set<String> propertyTypes,
            @Nullable Set<String> excludes, @Nullable String async) {
        return newFTIndexDefinition(index, type, name, propertyTypes, excludes,
                async, null);
    }

    public static NodeBuilder newFTIndexDefinition(
            @NotNull NodeBuilder index, @NotNull String name, String type,
            @Nullable Set<String> propertyTypes,
            @Nullable Set<String> excludes, @Nullable String async,
            @Nullable Boolean stored) {
        if (index.hasChildNode(name)) {
            return index.child(name);
        }
        index = index.child(name);
        index.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, type)
                .setProperty(REINDEX_PROPERTY_NAME, true);
        if (async != null) {
            index.setProperty(ASYNC_PROPERTY_NAME, async);
        }
        if (propertyTypes != null && !propertyTypes.isEmpty()) {
            index.setProperty(createProperty(INCLUDE_PROPERTY_TYPES,
                    propertyTypes, STRINGS));
        }
        if (excludes != null && !excludes.isEmpty()) {
            index.setProperty(createProperty(EXCLUDE_PROPERTY_NAMES, excludes,
                    STRINGS));
        }
        if (stored != null) {
            index.setProperty(createProperty(EXPERIMENTAL_STORAGE, stored));
        }
        return index;
    }

    public static NodeBuilder newFTFileIndexDefinition(
            @NotNull NodeBuilder index, @NotNull String name, String type,
            @Nullable Set<String> propertyTypes, @NotNull String path) {
        return newFTFileIndexDefinition(index, type, name, propertyTypes, null,
                path, null);
    }

    public static NodeBuilder newFTFileIndexDefinition(
            @NotNull NodeBuilder index, @NotNull String name, String type,
            @Nullable Set<String> propertyTypes,
            @Nullable Set<String> excludes, @NotNull String path,
            @Nullable String async) {
        if (index.hasChildNode(name)) {
            return index.child(name);
        }
        index = index.child(name);
        index.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, type)
                .setProperty(PERSISTENCE_NAME, PERSISTENCE_FILE)
                .setProperty(PERSISTENCE_PATH, path)
                .setProperty(REINDEX_PROPERTY_NAME, true);
        if (async != null) {
            index.setProperty(ASYNC_PROPERTY_NAME, async);
        }
        if (propertyTypes != null && !propertyTypes.isEmpty()) {
            index.setProperty(createProperty(INCLUDE_PROPERTY_TYPES,
                    propertyTypes, STRINGS));
        }
        if (excludes != null && !excludes.isEmpty()) {
            index.setProperty(createProperty(EXCLUDE_PROPERTY_NAMES, excludes,
                    STRINGS));
        }
        return index;
    }

    public static NodeBuilder newFTPropertyIndexDefinition(
            @NotNull NodeBuilder index, @NotNull String name, String type,
            @NotNull Set<String> includes,
            @NotNull String async) {
        checkArgument(!includes.isEmpty(), "Fulltext property index " +
                "requires explicit list of property names to be indexed");

        index = index.child(name);
        index.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, type)
                .setProperty(REINDEX_PROPERTY_NAME, true);
        index.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        index.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, includes, STRINGS));

        if (async != null) {
            index.setProperty(ASYNC_PROPERTY_NAME, async);
        }
        return index;
    }

}
