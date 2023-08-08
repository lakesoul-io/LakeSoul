// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac;

import com.dmetasoul.lakesoul.meta.DBConnector;
import com.dmetasoul.lakesoul.meta.GlobalConfig;
import org.casbin.adapter.JDBCAdapter;
import org.casbin.jcasbin.main.SyncedEnforcer;
import org.casbin.jcasbin.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

public class AuthZEnforcer {
    private static final Logger LOG = LoggerFactory.getLogger(AuthZEnforcer.class);
    private static AuthZEnforcer instance = null;

    private SyncedEnforcer enforcer;

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


    private void initEnforcer() throws Exception {
        String modelValue = GlobalConfig.get().getAuthZCasbinModel();
        Model model = new Model();
        model.loadModelFromText(modelValue);

        // init casbin jdbc adapter
        JDBCAdapter a = new JDBCAdapter(DBConnector.getDS(), true, "casbin_rule", false);

        enforcer = new SyncedEnforcer(model, a);
        LOG.info("Casbin enforcer successfully initialized");
    }

    private AuthZEnforcer() {
        try {
            initEnforcer();
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
