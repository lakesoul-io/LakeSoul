package com.dmetasoul.lakesoul.meta.rbac;

public class AuthException extends RuntimeException {
    public AuthException(){
        super("lakesoul access denied!");
    }
}
