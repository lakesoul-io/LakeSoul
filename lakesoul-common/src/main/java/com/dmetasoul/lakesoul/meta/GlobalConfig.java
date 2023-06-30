/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    public static final String authZCasbinModelQueryKey = "lakesoul.authz.casbin.model.query";
    public static final String authZCasbinDBUrlKey = "lakesoul.authz.casbin.db.url";
    private static GlobalConfig instance = null;
    private boolean authZEnabled;
    private String authZCasbinModelQuery;
    private String authZCasbinDBUrl;

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

    public String getAuthZCasbinModelQuery() {
        return authZCasbinModelQuery;
    }

    public String getAuthZCasbinDBUrl() {
        return authZCasbinDBUrl;
    }

    private void loadAuthZConfig(PreparedStatement pstmt) throws SQLException {
        authZEnabled = getBooleanValue(authZEnabledKey, pstmt, authZEnabledDefault);
        if (authZEnabled) {
            authZCasbinModelQuery = getStringValue(authZCasbinModelQueryKey, pstmt, "");
            if (authZCasbinModelQuery.isEmpty()) {
                throw new IllegalArgumentException("AuthZ enabled but model table not set");
            }
            authZCasbinDBUrl = getStringValue(authZCasbinDBUrlKey, pstmt, "");
            if (authZCasbinDBUrl.isEmpty()) {
                throw new IllegalArgumentException("AuthZ enabled but policy table not set");
            }
        }
        LOG.info("AuthZ enabled {}, model: {}, policy: {}", authZEnabled, authZCasbinModelQuery, authZCasbinDBUrl);
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
