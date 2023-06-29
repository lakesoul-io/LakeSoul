package com.dmetasoul.lakesoul.meta.rbac;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface AuthZ {
    String value() default "";
    String object() default "object";
    String action() default "action";
}

