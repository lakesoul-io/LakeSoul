/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package com.dmetasoul.lakesoul.meta.rbac;

import com.dmetasoul.lakesoul.meta.rbac.fetcher.AuthZFetcher;
import org.aspectj.lang.JoinPoint;
import org.junit.Test;

import java.util.Collections;
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

     public static class TestClass{
         @AuthZ(object = "domain", action = "db_create")
         public boolean done(){
             return true;
         }

         @AuthZ(object = "db", action =  "tb_create_drop", fetcher = TestFetcher.class)
         public boolean done2(String dbName){
             return true;
         }

         @AuthZ(value = "db.tb_create_drop", fetcher = TestFetcher.class)
         public boolean done3(String dbName){
             return true;
         }

         @AuthZ(object = "db", action =  "tb_create_drop")
         @AuthZObject(index = 1)
         public boolean done4(int aaa, String dbName){
             return true;
         }

         @AuthZ(value = "db.tb_create_drop")
         @AuthZObject(name = "dbName")
         public boolean done5(int aaa, String dbName){
             return true;
         }
     }

     public static class TestFetcher implements AuthZFetcher {
         @Override
         public List<String> getObject(JoinPoint point) {
             String object = (String) point.getArgs()[0];
             return List.of(object);
         }
     }
}
