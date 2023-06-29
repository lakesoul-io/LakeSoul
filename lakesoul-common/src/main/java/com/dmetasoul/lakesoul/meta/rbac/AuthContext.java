package com.dmetasoul.lakesoul.meta.rbac;

public class AuthContext {

    public final String LAKE_SOUL_SUBJECT_ENV = "LAKE_SOUL_SUBJECT";
    public final String LAKE_SOUL_DOMAIN_ENV = "LAKE_SOUL_DOMAIN";
    private static final AuthContext CONTEXT =  new AuthContext();

    private AuthContext(){
        this.domain = System.getenv(LAKE_SOUL_SUBJECT_ENV);
        this.subject = System.getenv(LAKE_SOUL_DOMAIN_ENV);
        if(this.domain == null) {
            this.domain = "";
        }
        if(this.subject == null) {
            this.subject = "";
        }
    }

    public static AuthContext getInstance(){
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
