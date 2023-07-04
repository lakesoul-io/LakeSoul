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

import com.dmetasoul.lakesoul.meta.rbac.fetcher.AuthZFetcher;
import com.dmetasoul.lakesoul.meta.rbac.fetcher.AuthZNullFetcher;
import com.dmetasoul.lakesoul.meta.rbac.fetcher.AuthZParamFetcher;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;

import java.util.List;

@Aspect
public class AuthZAspect {

    AuthZAdvice advice;

    public AuthZAspect(){
        this.advice = new AuthZAdvice();
    }

    @Pointcut("execution(* *(..)) && @annotation(com.dmetasoul.lakesoul.meta.rbac.AuthZ)")
    public void pointcut(){

    }

    @Around("pointcut() && args(..)")
    public Object around(ProceedingJoinPoint joinPoint) throws Throwable {
        if(AuthZEnforcer.get() == null){
            // if authz is not enabled
            // proceed and return
            return joinPoint.proceed();
        }
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        AuthZ annotation = methodSignature.getMethod().getAnnotation(AuthZ.class);
        AuthZObject authZObject = methodSignature.getMethod().getAnnotation(AuthZObject.class);
        String object = annotation.object();
        String action = annotation.action();
        String value = annotation.value();
        if(value.contains(".")){
            String[] vals = annotation.value().split("\\.");
            object = vals[0];
            action = vals[1];
        }

        String subject = AuthZContext.getInstance().getSubject();
        String domain = AuthZContext.getInstance().getDomain();

        Class<? extends AuthZFetcher> fetcher = annotation.fetcher();
        AuthZFetcher authZFetcher = null;
        if(fetcher != AuthZNullFetcher.class){
            authZFetcher = fetcher.newInstance();
        }else if(authZObject != null) {
            authZFetcher = new AuthZParamFetcher(authZObject);
        }

        if(authZFetcher != null){
            List<String> objects = authZFetcher.getObject(joinPoint);
            for(String obj : objects){
                // override domain and object passed from outside
                String objFullName = object + "_" + obj;
                List<List<String>> policies = AuthZEnforcer.get().getFilteredNamedPolicy("p", 2, objFullName);
                if(policies.size() == 0) {
                    throw new AuthZException();
                }
                String objDomain = policies.get(0).get(1);
                if(!advice.hasPermit(subject, objDomain, objFullName, action)){
                    throw new AuthZException();
                }
            }
        }else {
            if(!advice.hasPermit(subject, domain, object, action)){
                throw new AuthZException();
            }
        }

        Object result = joinPoint.proceed();
        advice.after();
        return result;
    }
}
