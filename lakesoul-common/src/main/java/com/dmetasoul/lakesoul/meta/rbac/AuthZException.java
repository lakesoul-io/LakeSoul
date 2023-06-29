package com.dmetasoul.lakesoul.meta.rbac;

public class AuthZException extends RuntimeException {
    public AuthZException(){
        super("lakesoul access denied!");
    }
}
