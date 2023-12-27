// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class LakeSoulConfig {

    private static LakeSoulConfig instance = null;

    public static synchronized LakeSoulConfig getInstance() {
        return instance;
    }

    public static synchronized void initInstance(Map<String, String> config){
        instance = new LakeSoulConfig(config);
    }

    private LakeSoulConfig(Map<String, String> config){
        requireNonNull(config, "config should not be null!");
        this.accessKey = config.get("fs.s3a.access.key");
        this.accessSecret = config.get("fs.s3a.secret.key");
        this.region = config.get("fs.s3a.endpoint.region");
        this.bucketName = config.get("fs.s3a.bucket");
        this.endpoint = config.get("fs.s3a.endpoint");
        this.defaultFS = config.get("fs.defaultFS");
        this.user = config.get("fs.hdfs.user");
        this.virtualPathStyle = Boolean.parseBoolean(config.getOrDefault("fs.s3a.path.style.access", "false"));
        this.timeZone = config.getOrDefault("timezone","");
    }

    private String accessKey;
    private String accessSecret;
    private String region;
    private String bucketName;
    private String endpoint;
    private String user;
    private String defaultFS;
    private String timeZone;
    private boolean virtualPathStyle;


    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getAccessSecret() {
        return accessSecret;
    }

    public void setAccessSecret(String accessSecret) {
        this.accessSecret = accessSecret;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getDefaultFS() {
        return defaultFS;
    }

    public void setDefaultFS(String defaultFS) {
        this.defaultFS = defaultFS;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public boolean isVirtualPathStyle() {
        return virtualPathStyle;
    }

    public void setVirtualPathStyle(boolean virtualPathStyle) {
        this.virtualPathStyle = virtualPathStyle;
    }
}
