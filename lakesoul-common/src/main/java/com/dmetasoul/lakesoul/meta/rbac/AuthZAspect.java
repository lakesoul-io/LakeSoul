package com.dmetasoul.lakesoul.meta.rbac;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;

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

        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        AuthZ annotation = methodSignature.getMethod().getAnnotation(AuthZ.class);
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

        throw new AuthZException();
    }
}
