// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

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
