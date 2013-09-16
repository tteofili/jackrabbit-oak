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
package org.apache.jackrabbit.oak.spi.security;

import java.util.Collections;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConfigurationParameters... TODO
 */
public class ConfigurationParameters {

    private static final Logger log = LoggerFactory.getLogger(ConfigurationParameters.class);

    public static final ConfigurationParameters EMPTY = new ConfigurationParameters();

    private final Map<String, Object> options;

    public ConfigurationParameters() {
        this(null);
    }

    public ConfigurationParameters(@Nullable Map<String, ?> options) {
        this.options = (options == null) ? Collections.<String, Object>emptyMap() : Collections.unmodifiableMap(options);
    }

    public static ConfigurationParameters newInstance(@Nonnull ConfigurationParameters... params) {
        Map<String, Object> m = Maps.newHashMap();
        for (ConfigurationParameters cp : params) {
            m.putAll(cp.options);
        }
        return new ConfigurationParameters(ImmutableMap.copyOf(m));
    }

    public static ConfigurationParameters newInstance(@Nonnull Properties properties) {
        if (properties.isEmpty()) {
            return EMPTY;
        }

        Map<String, Object> options = new HashMap<String, Object>(properties.size());
        for (String name : properties.stringPropertyNames()) {
            options.put(name, properties.getProperty(name));
        }
        return new ConfigurationParameters(options);
    }

    public static ConfigurationParameters newInstance(@Nonnull Dictionary<String, Object> properties) {
        if (properties.isEmpty()) {
            return EMPTY;
        }

        Map<String, Object> options = new HashMap<String, Object>(properties.size());
        for (Enumeration<String> keys = properties.keys(); keys.hasMoreElements();) {
            String key = keys.nextElement();
            options.put(key, properties.get(key));
        }
        return new ConfigurationParameters(options);
    }

    /**
     * Returns {@code true} if this instance contains a configuration entry with
     * the specified key irrespective of the defined value; {@code false} otherwise.
     *
     * @param key The key to be tested.
     * @return {@code true} if this instance contains a configuration entry with
     * the specified key irrespective of the defined value; {@code false} otherwise.
     */
    public boolean contains(@Nonnull String key) {
        return options.containsKey(key);
    }

    /**
     * Returns the value of the configuration entry with the given {@code key}
     * applying the following rules:
     *
     * <ul>
     *     <li>If this instance doesn't contain a configuration entry with that
     *     key the specified {@code defaultValue} will be returned.</li>
     *     <li>If {@code defaultValue} is {@code null} the original value will
     *     be returned.</li>
     *     <li>If the configured value is {@code null} this method will always
     *     return {@code null}.</li>
     *     <li>If neither {@code defaultValue} nor the configured value is
     *     {@code null} an attempt is made to convert the configured value to
     *     match the type of the default value.</li>
     * </ul>
     *
     *
     * @param key The name of the configuration option.
     * @param defaultValue The default value to return if no such entry exists
     * or to use for conversion.
     * @param targetClass The target class
     * @return The original or converted configuration value or {@code null}.
     */
    @CheckForNull
    public <T> T getConfigValue(@Nonnull String key, @Nullable T defaultValue,
                                @Nullable Class<T> targetClass) {
        if (options.containsKey(key)) {
            return convert(options.get(key), defaultValue, targetClass);
        } else {
            return defaultValue;
        }
    }

    @Nonnull
    public <T> T getConfigValue(@Nonnull String key, @Nonnull T defaultValue) {
        if (options.containsKey(key)) {
            T value = convert(options.get(key), defaultValue, null);
            return (value == null) ? defaultValue : value;
        } else {
            return defaultValue;
        }
    }

    //--------------------------------------------------------< private >---
    @SuppressWarnings("unchecked")
    @Nullable
    private static <T> T convert(@Nullable Object configProperty, @Nullable T defaultValue, @Nullable Class<T> trgtClass) {
        if (configProperty == null) {
            return null;
        }

        T value;
        String str = configProperty.toString();
        Class targetClass;
        if (trgtClass != null) {
            targetClass = trgtClass;
        } else {
            targetClass = (defaultValue == null) ? configProperty.getClass() : defaultValue.getClass();
        }
        try {
            if (targetClass.equals(configProperty.getClass()) || targetClass.isAssignableFrom(configProperty.getClass())) {
                value = (T) configProperty;
            } else if (targetClass == String.class) {
                value = (T) str;
            } else if (targetClass == Integer.class) {
                value = (T) Integer.valueOf(str);
            } else if (targetClass == Long.class) {
                value = (T) Long.valueOf(str);
            } else if (targetClass == Double.class) {
                value = (T) Double.valueOf(str);
            } else if (targetClass == Boolean.class) {
                value = (T) Boolean.valueOf(str);
            } else {
                // unsupported target type
                log.warn("Unsupported target type {} for value {}", targetClass.getName(), str);
                throw new IllegalArgumentException("Cannot convert config entry " + str + " to " + targetClass.getName());
            }
        } catch (NumberFormatException e) {
            log.warn("Invalid value {}; cannot be parsed into {}", str, targetClass.getName());
            throw new IllegalArgumentException("Cannot convert config entry " + str + " to " + targetClass.getName());
        }
        return value;
    }
}