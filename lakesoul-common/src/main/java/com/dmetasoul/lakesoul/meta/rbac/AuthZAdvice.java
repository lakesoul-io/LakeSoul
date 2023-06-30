package com.dmetasoul.lakesoul.meta.rbac;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthZAdvice {
    
    private static final Logger LOG = LoggerFactory.getLogger(AuthZAdvice.class);
    private static final String LOG_TEMPLATE = "authing: domain = %s, subject = %s, object = %s, action = %s";

    public boolean hasPermit(String object, String action){
        String domain = AuthZContext.getInstance().getDomain();
        String subject = AuthZContext.getInstance().getSubject();
        LOG.info(String.format(LOG_TEMPLATE, domain, subject, object, action));
        System.out.println(String.format(LOG_TEMPLATE, domain, subject, object, action));
        return AuthZEnforcer.get() == null || AuthZEnforcer.get().enforce(subject, domain, object, action);
    }

    public void after(){
        
    }

}
