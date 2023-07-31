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
