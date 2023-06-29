package com.dmetasoul.lakesoul.meta.rbac;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;

@Aspect
public class AuthAspect {

    AuthAdvice advice;

    public AuthAspect(){
        this.advice = new AuthAdvice();
    }

    @Pointcut("execution(* *(..)) && @annotation(com.dmetasoul.lakesoul.meta.rbac.Auth)")
    public void pointcut(){

    }

    @Around("pointcut() && args(..)")
    public Object around(ProceedingJoinPoint joinPoint) throws Throwable {

        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Auth annotation = methodSignature.getMethod().getAnnotation(Auth.class);
        String object = annotation.object();
        String action = annotation.action();
        String value = annotation.value();
        if(!value.equals("") && value.contains(".")){
            String[] vals = annotation.value().split(".");
            object = vals[0];
            action = vals[1];
        }

        if(advice.hasPermit(object, action)){
            Object result = joinPoint.proceed();
            advice.after();
            return result;
        }

        throw new AuthException();
    }
}
