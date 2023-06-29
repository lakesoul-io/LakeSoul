package com.dmetasoul.lakesoul.meta.rbac;

import org.junit.Test;

public class AuthTest {

    @Test
    public void testAnotation(){
        try{
            if(new TestClass().done()){
                throw new RuntimeException("should not be true");
            }
        }catch(AuthException ignored){
        }
    }

    @Test
    public void testAuthPass(){
        AuthContext.getInstance().setSubject("yuanf");
        AuthContext.getInstance().setDomain("default");
        assert new TestClass().done();
    }

    public static class TestClass{
        @Auth(object = "database", action =  "create")
        public boolean done(){
            return true;
        }
    }

}
