package com.dmetasoul.lakesoul.meta.rbac;

public class AuthZContext {

    public final String LAKESOUL_AUTHZ_SUBJECT_ENV = "LAKESOUL_AUTHZ_SUBJECT";
    public final String LAKESOUL_AUTHZ_DOMAIN_ENV = "LAKESOUL_AUTHZ_DOMAIN";
    private static final AuthZContext CONTEXT =  new AuthZContext();

    private AuthZContext(){
        this.domain = System.getenv(LAKESOUL_AUTHZ_SUBJECT_ENV);
        this.subject = System.getenv(LAKESOUL_AUTHZ_DOMAIN_ENV);
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
