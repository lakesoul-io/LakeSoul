// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GlobalConfig {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalConfig.class);
    public static final String authZEnabledKey = "lakesoul.authz.enabled";
    public static final boolean authZEnabledDefault = false;
    public static final String authZCasbinModelKey = "lakesoul.authz.casbin.model";
    private static GlobalConfig instance = null;
    private boolean authZEnabled;
    private String authZCasbinModel;

    private GlobalConfig() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement("select value from global_config where key=?");

            loadAuthZConfig(pstmt);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }

    public static synchronized GlobalConfig get() {
        if (instance == null) {
            instance = new GlobalConfig();
        }
        return instance;
    }

    public boolean isAuthZEnabled() {
        return authZEnabled;
    }

    public void setAuthZEnabled(boolean enabled){
        this.authZEnabled = enabled;
    }

    public String getAuthZCasbinModel() {
        return authZCasbinModel;
    }

    private void loadAuthZConfig(PreparedStatement pstmt) throws SQLException {
        authZEnabled = getBooleanValue(authZEnabledKey, pstmt, authZEnabledDefault);
        if (authZEnabled) {
            authZCasbinModel = getStringValue(authZCasbinModelKey, pstmt, "");
            if (authZCasbinModel.isEmpty()) {
                throw new IllegalArgumentException("AuthZ enabled but model table not set");
            }
        }
        LOG.info("AuthZ enabled {}, model: {}", authZEnabled, authZCasbinModel);
    }

    private ResultSet getResultSet(String key, PreparedStatement pstmt) throws SQLException {
        pstmt.setString(1, key);
        return pstmt.executeQuery();
    }

    private String getStringValue(String key, PreparedStatement pstmt, String defaultValue) throws SQLException {
        ResultSet rs = getResultSet(key, pstmt);
        if (rs.next()) {
            String value = rs.getString("value");
            if (value == null || value.isEmpty()) {
                return defaultValue;
            } else {
                return value;
            }
        } else {
            return defaultValue;
        }
    }

    private boolean getBooleanValue(String key, PreparedStatement pstmt, boolean defaultValue) throws SQLException {
        ResultSet rs = getResultSet(key, pstmt);
        if (rs.next()) {
            String value = rs.getString("value");
            if (value == null || value.isEmpty()) {
                return defaultValue;
            } else {
                if (value.equalsIgnoreCase("true")) {
                    return true;
                } else if (value.equalsIgnoreCase("false")) {
                    return false;
                } else {
                    return defaultValue;
                }
            }
        } else {
            return defaultValue;
        }
    }
}
