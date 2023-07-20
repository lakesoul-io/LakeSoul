/*
 * Copyright [2022] [DMetaSoul Team]
 *
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
 *
 */

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
            this.domain = "";
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
