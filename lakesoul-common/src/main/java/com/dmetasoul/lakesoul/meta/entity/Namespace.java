/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */
package com.dmetasoul.lakesoul.meta.entity;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.Validate;

import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Namespace of tables
 */
public class Namespace {

    /**
     * Dot-separated-formatted namespace
     */
    private String namespace;

    private JSONObject properties = new JSONObject();

    private String comment;

    private static final Namespace EMPTY_NAMESPACE = new Namespace();

    private static final Namespace DEFAULT_NAMESPACE = new Namespace(new String[]{"default"});

    private static final Predicate<String> CONTAINS_NULL_CHARACTER =
            Pattern.compile("\u0000", Pattern.UNICODE_CHARACTER_CLASS).asPredicate();

    public static Namespace empty() {
        return EMPTY_NAMESPACE;
    }

    public static Namespace defaultNamespace() {
        return DEFAULT_NAMESPACE;
    }

    public static Namespace of(String... levels) {
        Validate.isTrue(null != levels, "Cannot create Namespace from null array");
        if (levels.length == 0) {
            return empty();
        }

        for (String level : levels) {
            Validate.notNull(level, "Cannot create a namespace with a null level");
            Validate.isTrue(
                    !CONTAINS_NULL_CHARACTER.test(level),
                    "Cannot create a namespace with the null-byte character");
        }

        return new Namespace(levels);
    }

    private final String[] levels;

    public Namespace(String[] levels) {
        this.levels = levels;
        this.namespace = String.join(".", levels);
    }

    public Namespace(String namespace) {
        this.levels = namespace.split("\\.");
        this.namespace = namespace;
    }

    public Namespace() {
        this(new String[]{});
    }

    public String[] getLevels() {
        return levels;
    }


    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public JSONObject getProperties() {
        return properties;
    }

    public void setProperties(JSONObject properties) {
        this.properties = properties;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }
}
