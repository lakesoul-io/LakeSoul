// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac.fetcher;

import com.dmetasoul.lakesoul.meta.rbac.AuthZBefore;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.reflect.MethodSignature;

import java.util.LinkedList;
import java.util.List;

/**
 * fetch object name from method arguments
 */
public class AuthZParamFetcher implements AuthZFetcher<JoinPoint> {

    AuthZBefore authZBefore;

    public AuthZParamFetcher(AuthZBefore authZBefore){
        this.authZBefore = authZBefore;
    }

    @Override
    public List<String> getObject(JoinPoint point) {
        List<String> objects = new LinkedList<>();
        int idx = authZBefore.index();
        if(!authZBefore.name().trim().equals("")){
            MethodSignature signature = (MethodSignature) point.getSignature();
            String[] parameterNames = signature.getParameterNames();
            for(int i = 0; i < parameterNames.length; i++){
                if(parameterNames[i].equals(authZBefore.name().trim())){
                    idx = i;
                    break;
                }
            }

        }
        objects.add(point.getArgs()[idx].toString());
        return objects;
    }
}
