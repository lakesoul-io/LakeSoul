package com.dmetasoul.lakesoul.meta.entity;

import com.alibaba.fastjson.JSONObject;

public class Namespace {
    private String name;

    private JSONObject properties;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public JSONObject getProperties() {
        return properties;
    }

    public void setProperties(JSONObject properties) {
        this.properties = properties;
    }
}
