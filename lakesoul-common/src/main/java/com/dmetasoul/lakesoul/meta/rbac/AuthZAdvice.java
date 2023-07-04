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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class AuthZAdvice {
    
    private static final Logger LOG = LoggerFactory.getLogger(AuthZAdvice.class);
    private static final String LOG_TEMPLATE = "authing: domain = %s, subject = %s, object = %s, action = %s";

    public boolean hasPermit(String subject, String domain, String object, String action){
        LOG.info(String.format(LOG_TEMPLATE, domain, subject, object, action));
        System.out.printf((LOG_TEMPLATE) + "%n", domain, subject, object, action);
        return Objects.requireNonNull(AuthZEnforcer.get()).enforce(subject, domain, object, action);
    }

    public void after(){
        
    }

}
