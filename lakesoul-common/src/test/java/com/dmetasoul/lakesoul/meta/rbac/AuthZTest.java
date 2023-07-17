// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac;

import com.dmetasoul.lakesoul.meta.rbac.fetcher.AuthZFetcher;
import org.aspectj.lang.JoinPoint;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class AuthZTest {

//     @Test
//     public void testAnotation(){
//         try{
//             if(new TestClass().done()){
//                 throw new RuntimeException("should not be true");
//             }
//         }catch(AuthZException ignored){
//         }
//     }
//
//     @Test
//     public void testAuthPass(){
//         AuthZContext.getInstance().setSubject("yuanf");
//         AuthZContext.getInstance().setDomain("test1");
//         assert new TestClass().done();
//     }
//
//    @Test
//    public void testAuthPass2(){
//        AuthZContext.getInstance().setSubject("yuanf");
//        AuthZContext.getInstance().setDomain("test1");
//        assert new TestClass().done2("db1");
//    }
//
//    @Test
//    public void testAuthPass3(){
//        AuthZContext.getInstance().setSubject("yuanf");
//        AuthZContext.getInstance().setDomain("test1");
//        assert new TestClass().done3("db1");
//    }
//
//    @Test
//    public void testAuthAnotaionObjectPass1(){
//        AuthZContext.getInstance().setSubject("yuanf");
//        AuthZContext.getInstance().setDomain("test1");
//        assert new TestClass().done4(0, "db1");
//    }
//
//    @Test
//    public void testAuthAnotaionObjectPass2(){
//        AuthZContext.getInstance().setSubject("yuanf");
//        AuthZContext.getInstance().setDomain("test1");
//        assert new TestClass().done5(0, "db1");
//    }
//
//    @Test
//    public void testAuthAfter(){
//        AuthZContext.getInstance().setSubject("yuanf");
//        AuthZContext.getInstance().setDomain("test1");
//        new TestClass().done6();
//    }
//
//     public static class TestClass{
//         @AuthZ(object = "domain", action = "db_create")
//         public boolean done(){
//             return true;
//         }
//
//         @AuthZ(object = "db", action =  "tb_create_drop")
//         @AuthZBefore(fetcher = TestFetcher.class)
//         public boolean done2(String dbName){
//             return true;
//         }
//
//         @AuthZ(value = "db.tb_create_drop")
//         @AuthZBefore(fetcher = TestFetcher.class)
//         public boolean done3(String dbName){
//             return true;
//         }
//
//         @AuthZ(object = "db", action =  "tb_create_drop")
//         @AuthZBefore(index = 1)
//         public boolean done4(int aaa, String dbName){
//             return true;
//         }
//
//         @AuthZ(value = "db.tb_create_drop")
//         @AuthZBefore(name = "dbName")
//         public boolean done5(int aaa, String dbName){
//             return true;
//         }
//
//         @AuthZ(value = "db.tb_create_drop")
//         @AuthZAfter
//         public String done6(){
//             return "db1";
//         }
//     }
//
//     public static class TestFetcher implements AuthZFetcher<JoinPoint> {
//         @Override
//         public List<String> getObject(JoinPoint point) {
//             String object = (String) point.getArgs()[0];
//             LinkedList<String> objects = new LinkedList<>();
//             objects.add(object);
//             return objects;
//         }
//     }
}

