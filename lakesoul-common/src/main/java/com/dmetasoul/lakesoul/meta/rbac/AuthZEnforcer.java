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

package com.dmetasoul.lakesoul.meta.rbac;

import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.DataBaseProperty;
import com.dmetasoul.lakesoul.meta.GlobalConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.casbin.adapter.JDBCAdapter;
import org.casbin.jcasbin.main.SyncedEnforcer;
import org.casbin.jcasbin.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class AuthZEnforcer {
    private static final Logger LOG = LoggerFactory.getLogger(AuthZEnforcer.class);
    private static AuthZEnforcer instance = null;

    public SyncedEnforcer enforcer;

    public static synchronized SyncedEnforcer get() {
        if (GlobalConfig.get().isAuthZEnabled()) {
            if (instance == null) {
                instance = new AuthZEnforcer();
            }
            return instance.enforcer;
        } else {
            return null;
        }
    }

    public static boolean authZEnabled() {
        return !(get() == null);
    }

    private DataSource ds;

    private void initDataSource() {
        DataBaseProperty dataBaseProperty = DBUtil.getDBInfo();
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(dataBaseProperty.getDriver());
        config.setJdbcUrl(GlobalConfig.get().getAuthZCasbinDBUrl());
        DBUtil.fillDataSourceConfig(config);
        ds = new HikariDataSource(config);
        LOG.info("Casbin datasource initialized");
    }

    private void initEnforcer() throws Exception {
        Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement();
        String modelQuery = GlobalConfig.get().getAuthZCasbinModelQuery();
        ResultSet rs = stmt.executeQuery(modelQuery);
        if (rs.next()) {
            String modelConfValue = rs.getString(1);
            LOG.info("Casbin model: {}", modelConfValue);

            // init casbin model
            Model model = new Model();
            model.loadModelFromText(modelConfValue);

            // init casbin jdbc adapter
            JDBCAdapter a = new JDBCAdapter(ds);

            enforcer = new SyncedEnforcer(model, a);
            LOG.info("Casbin enforcer successfully initialized");
        } else {
            throw new IllegalArgumentException("Cannot fetch casbin model config");
        }
    }

    private AuthZEnforcer() {
        try {
            initDataSource();
            initEnforcer();
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
