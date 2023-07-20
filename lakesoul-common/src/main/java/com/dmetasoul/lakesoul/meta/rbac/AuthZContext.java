// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac;

import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.GlobalConfig;

public class AuthZContext {

    public final String LAKESOUL_AUTHZ_SUBJECT_ENV = "LAKESOUL_AUTHZ_SUBJECT";
    public final String LAKESOUL_AUTHZ_DOMAIN_ENV = "LAKESOUL_AUTHZ_DOMAIN";
    private static final AuthZContext CONTEXT =  new AuthZContext();

    private AuthZContext(){
        this.domain = DBUtil.getDBInfo().getUsername();
        this.subject = DBUtil.getDomain();
        if(this.domain == null) {
            this.domain = "public";
        }
        if(this.subject == null) {
            this.subject = "";
        }
    }

    public static AuthZContext getInstance(){
        return CONTEXT;
    }

    private String subject;
    private String domain;

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

}
